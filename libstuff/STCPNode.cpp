#include "libstuff.h"
// #include <execinfo.h> // for backtrace
#undef SLOGPREFIX
#define SLOGPREFIX "{" << name << "} "

STCPNode::STCPNode(const string& name_, const string& host, const uint64_t recvTimeout_)
    : STCPServer(host), name(name_), recvTimeout(recvTimeout_) {
}

STCPNode::~STCPNode() {
    // Clean up all the sockets and peers
    for (Socket* socket : acceptedSocketList) {
        closeSocket(socket);
    }
    acceptedSocketList.clear();
    for (Peer* peer : peerList) {
        // Shut down the peer
        peer->closeSocket(this);
        delete peer;
    }
    peerList.clear();
}

const string& STCPNode::stateName(STCPNode::State state) {
    // This returns the legacy names MASTERING/SLAVING until all nodes have been updated to be able to
    // understand the new LEADING/FOLLOWING names.
    static string placeholder = "";
    static map<State, string> lookup = {
        {UNKNOWN, "UNKNOWN"},
        {SEARCHING, "SEARCHING"},
        {SYNCHRONIZING, "SYNCHRONIZING"},
        {WAITING, "WAITING"},
        {STANDINGUP, "STANDINGUP"},
        {LEADING, "MASTERING"},
        {STANDINGDOWN, "STANDINGDOWN"},
        {SUBSCRIBING, "SUBSCRIBING"},
        {FOLLOWING, "SLAVING"},
    };
    auto it = lookup.find(state);
    if (it == lookup.end()) {
        return placeholder;
    } else {
        return it->second;
    }
}

STCPNode::State STCPNode::stateFromName(const string& name) {
    string normalizedName = SToUpper(name);

    // Accept both old and new state names, but map them all to the new states.
    static map<string, State> lookup = {
        {"SEARCHING", SEARCHING},
        {"SYNCHRONIZING", SYNCHRONIZING},
        {"WAITING", WAITING},
        {"STANDINGUP", STANDINGUP},
        {"LEADING", LEADING},
        {"MASTERING", LEADING},
        {"STANDINGDOWN", STANDINGDOWN},
        {"SUBSCRIBING", SUBSCRIBING},
        {"FOLLOWING", FOLLOWING},
        {"SLAVING", FOLLOWING},
    };
    auto it = lookup.find(normalizedName);
    if (it == lookup.end()) {
        return UNKNOWN;
    } else {
        return it->second;
    }
}

void STCPNode::addPeer(const string& peerName, const string& host, const STable& params) {
    // Create a new peer and ready it for connection
    SASSERT(SHostIsValid(host));
    SINFO("Adding peer #" << peerList.size() << ": " << peerName << " (" << host << "), " << SComposeJSONObject(params));
    Peer* peer = new Peer(peerName, host, params, peerList.size() + 1);

    // Wait up to 2s before trying the first time
    peer->nextReconnect = STimeNow() + SRandom::rand64() % (STIME_US_PER_S * 2);
    peerList.push_back(peer);
}

STCPNode::Peer* STCPNode::getPeerByID(uint64_t id) {
    if (id && id <= peerList.size()) {
        return peerList[id - 1];
    }
    return nullptr;
}

uint64_t STCPNode::getIDByPeer(STCPNode::Peer* peer) {
    uint64_t id = 1;
    for (auto p : peerList) {
        if (p == peer) {
            return id;
        }
        id++;
    }
    return 0;
}

void STCPNode::prePoll(fd_map& fdm) {
    // Let the base class do its thing
    return STCPServer::prePoll(fdm);
}

void STCPNode::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    // Process the sockets
    STCPServer::postPoll(fdm);

    // Accept any new peers
    Socket* socket = nullptr;
    while ((socket = acceptSocket()))
        acceptedSocketList.push_back(socket);

    // Process the incoming sockets
    list<Socket*>::iterator nextSocketIt = acceptedSocketList.begin();
    while (nextSocketIt != acceptedSocketList.end()) {
        // See if we've logged in (we know we're already connected because
        // we're accepting an inbound connection)
        list<Socket*>::iterator socketIt = nextSocketIt++;
        Socket* socket = *socketIt;
        try {
            // Verify it's still alive
            if (socket->state.load() != Socket::CONNECTED)
                STHROW("premature disconnect");

            // Still alive; try to login
            SData message;
            int messageSize = message.deserialize(socket->recvBuffer);
            if (messageSize) {
                // What is it?
                SConsumeFront(socket->recvBuffer, messageSize);
                if (SIEquals(message.methodLine, "NODE_LOGIN")) {
                    // Got it -- can we asssociate with a peer?
                    bool foundIt = false;
                    for (Peer* peer : peerList) {
                        // Just match any unconnected peer
                        // **FIXME: Authenticate and match by public key
                        if (peer->name == message["Name"]) {
                            // Found it!  Are we already connected?
                            if (!peer->s) {
                                // Attach to this peer and LOGIN
                                PINFO("Attaching incoming socket");
                                peer->s = socket;
                                peer->failedConnections = 0;
                                acceptedSocketList.erase(socketIt);
                                foundIt = true;

                                // Send our own PING back so we can estimate latency
                                _sendPING(peer);

                                // Let the child class do its connection logic
                                _onConnect(peer);
                                break;
                            } else
                                STHROW("already connected");
                        }
                    }

                    // Did we find it?
                    if (!foundIt) {
                        // This node wasn't expected
                        SWARN("Unauthenticated node '" << message["Name"] << "' attempted to connected, rejecting.");
                        STHROW("unauthenticated node");
                    }
                } else
                    STHROW("expecting NODE_LOGIN");
            }
        } catch (const SException& e) {
            // Died prematurely
            if (socket->recvBuffer.empty() && socket->sendBufferEmpty()) {
                SDEBUG("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), empty buffers");
            } else {
                SWARN("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), recv='"
                      << socket->recvBuffer << "', send='" << socket->sendBufferCopy() << "'");
            }
            closeSocket(socket);
            acceptedSocketList.erase(socketIt);
        }
    }

    // Try to establish connections with peers and process messages
    for (Peer* peer : peerList) {
        // See if we're connected
        if (peer->s) {
            // We have a socket; process based on its state
            switch (peer->s->state.load()) {
            case Socket::CONNECTED: {
                // See if there is anything new.
                peer->failedConnections = 0; // Success; reset failures
                SData message;
                int messageSize = 0;
                try {
                    // peer->s->lastRecvTime is always set, it's initialized to STimeNow() at creation.
                    if (peer->s->lastRecvTime + recvTimeout < STimeNow()) {
                        // Reset and reconnect.
                        SHMMM("Connection with peer '" << peer->name << "' timed out.");
                        STHROW("Timed Out!");
                    }

                    // Send PINGs 5s before the socket times out
                    if (STimeNow() - peer->s->lastSendTime > recvTimeout - 5 * STIME_US_PER_S) {
                        // Let's not delay on flushing the PING PONG exchanges
                        // in case we get blocked before we get to flush later.
                        SINFO("Sending PING to peer '" << peer->name << "'");
                        _sendPING(peer);
                    }

                    // Process all messages
                    while ((messageSize = message.deserialize(peer->s->recvBuffer))) {
                        // Which message?
                        SConsumeFront(peer->s->recvBuffer, messageSize);
                        if (peer->s->recvBuffer.size() > 10'000) {
                            // Make in known if this buffer ever gets big.
                            PINFO("Received '" << message.methodLine << "'(size: " << messageSize << ") with " 
                                  << (peer->s->recvBuffer.size()) << " bytes remaining in message buffer.");
                        } else {
                            PDEBUG("Received '" << message.methodLine << "'.");
                        }
                        if (SIEquals(message.methodLine, "PING")) {
                            // Let's not delay on flushing the PING PONG
                            // exchanges in case we get blocked before we
                            // get to flush later.  Pass back the remote
                            // timestamp of the PING such that the remote
                            // host can calculate latency.
                            SINFO("Received PING from peer '" << peer->name << "'. Sending PONG.");
                            SData pong("PONG");
                            pong["Timestamp"] = message["Timestamp"];
                            peer->s->send(pong.serialize());
                        } else if (SIEquals(message.methodLine, "PONG")) {
                            // Recevied the PONG; update our latency estimate for this peer.
                            // We set a lower bound on this at 1, because even though it should be pretty impossible
                            // for this to be 0 (it's in us), we rely on it being non-zero in order to connect to
                            // peers.
                            peer->latency = max(STimeNow() - message.calc64("Timestamp"), (uint64_t)1);
                            SINFO("Received PONG from peer '" << peer->name << "' (" << peer->latency/1000 << "ms latency)");
                        } else {
                            // Not a PING or PONG; pass to the child class
                            _onMESSAGE(peer, message);
                        }
                    }
                } catch (const SException& e) {
                    // Warn if the message is set. Otherwise, the error is that we got no message (we timed out), just
                    // reconnect without complaining about it.
                    if (message.methodLine.size()) {
                        PWARN("Error processing message '" << message.methodLine << "' (" << e.what()
                                                           << "), reconnecting:" << message.serialize());
                    }
                    SData reconnect("RECONNECT");
                    reconnect["Reason"] = e.what();
                    peer->s->send(reconnect.serialize());
                    shutdownSocket(peer->s);
                    break;
                }
                break;
            }

            case Socket::CLOSED: {
                // Done; clean up and try to reconnect
                uint64_t delay = SRandom::rand64() % (STIME_US_PER_S * 5);
                if (peer->s->connectFailure) {
                    PINFO("Peer connection failed after " << (STimeNow() - peer->s->openTime) / 1000
                                                          << "ms, reconnecting in " << delay / 1000 << "ms");
                } else {
                    PHMMM("Lost peer connection after " << (STimeNow() - peer->s->openTime) / 1000
                                                        << "ms, reconnecting in " << delay / 1000 << "ms");
                }
                _onDisconnect(peer);
                if (peer->s->connectFailure)
                    peer->failedConnections++;
                peer->closeSocket(this);
                peer->reset();
                peer->nextReconnect = STimeNow() + delay;
                nextActivity = min(nextActivity, peer->nextReconnect);
                break;
            }

            default:
                // Connecting or shutting down, wait
                // **FIXME: Add timeout here?
                break;
            }
        } else {
            // Not connected, is it time to try again?
            if (STimeNow() > peer->nextReconnect) {
                // Try again
                PINFO("Retrying the connection");
                peer->reset();
                peer->s = openSocket(peer->host);
                if (peer->s) {
                    // Try to log in now.  Send a PING immediately after so we
                    // can get a fast estimate of latency.
                    SData login("NODE_LOGIN");
                    login["Name"] = name;
                    peer->s->send(login.serialize());
                    _sendPING(peer);
                    _onConnect(peer);
                } else {
                    // Failed to open -- try again later
                    SWARN("Failed to open socket '" << peer->host << "', trying again in 60s");
                    peer->failedConnections++;
                    peer->nextReconnect = STimeNow() + STIME_US_PER_M;
                }
            } else {
                // Waiting to reconnect -- notify the caller
                nextActivity = min(nextActivity, peer->nextReconnect);
            }
        }
    }
}

void STCPNode::_sendPING(Peer* peer) {
    // Send a PING message, including our current timestamp
    SASSERT(peer);
    SData ping("PING");
    ping["Timestamp"] = SToStr(STimeNow());
    peer->s->send(ping.serialize());
}

void STCPNode::Peer::sendMessage(const SData& message) {
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        s->send(message.serialize());
    } else {
        SWARN("Tried to send " << message.methodLine << " to peer, but not available.");
    }
}

void STCPNode::Peer::closeSocket(STCPManager* manager) {
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        manager->closeSocket(s);
        s = nullptr;
    } else {
        SWARN("Peer " << name << " has no socket.");
    }
}
