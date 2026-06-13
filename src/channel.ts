import { StratoSocket, MessageData, IReceiveOpts } from "./socket";
import { throwCancellationIfAborted } from "./util/async";
import { PING_PAYLOAD, CONNECTION_NS } from "./util/protocol";

export interface IChannelOptions {
    destination?: string;
}

/**
 * High-level namespace-based Channel abstraction
 */
export class StratoChannel {
    private hasConnected = false;

    constructor(
        private readonly socket: StratoSocket,
        public readonly namespace: string,
        private readonly options: IChannelOptions = {},
    ) { }

    public get isConnected() {
        return this.socket.isConnected;
    }

    /**
     * The app session (`transportId`) this Channel is targetting, if
     * any. Guaranteed not-undefined when created via [StratoApp.channel]
     */
    public get destination() {
        return this.options.destination;
    }

    /**
     * Await and return a single packet received on this channel.
     * @throws CancellationError if an `AbortSignal` is provided to `opts`
     * that gets aborted before this receives anything.
     */
    public async receiveOne(opts: IReceiveOpts = {}) {
        for await (const received of this.receive(opts)) {
            return received;
        }

        throwCancellationIfAborted(opts.signal);

        throw new Error("Failed to receive any packet");
    }

    /**
     * Listen to all packets received on this channel. If the receive is aborted
     * via `AbortSignal`, this method simply stops emitting.
     */
    public async* receive(opts: IReceiveOpts = {}) {
        for await (const received of this.socket.receive(opts)) {
            if (received.namespace !== this.namespace) continue;
            if (
                this.options.destination
                && this.options.destination !== received.source
            ) {
                continue;
            }

            yield received;
        }
    }

    /**
     * Send a JSON-style message object and await the response
     * (expecting a parsed JSON object)
     * @throws CancellationError If this was aborted via `opts.signal` before
     * receiving a response.
     * @throws Error if we fail to receive a response otherwise.
     */
    public async send(message: Record<string, unknown>, opts: IReceiveOpts = {}) {
        const toSend = {
            requestId: this.socket.nextId(),
            ...message,
        };

        await this.write(toSend);

        // --- BUGFIX: Maak een controller aan om de onderliggende socket netjes af te breken ---
        const controller = new AbortController();
        const onUserAbort = () => controller.abort();

        if (opts.signal) {
            if (opts.signal.aborted) throwCancellationIfAborted(opts.signal);
            opts.signal.addEventListener("abort", onUserAbort);
        }

        try {
            // We sturen ons eigen controller.signal mee ipv de onbeschermde opts.signal
            for await (const m of this.receive({ ...opts, signal: controller.signal })) {
                if (Buffer.isBuffer(m.data) || typeof m.data === "string") {
                    continue;
                }

                // special cases:
                if (message === PING_PAYLOAD && m.data.type === "PONG") {
                    return m.data;
                }

                // otherwise...
                if (m.data.requestId === toSend.requestId) {
                    return m.data;
                }
            }
        } finally {
            // GARANTIE: Ruim de iterator/luisteraar onmiddellijk op in het socket object
            controller.abort();
            
            if (opts.signal) {
                opts.signal.removeEventListener("abort", onUserAbort);
            }
        }
        // -----------------------------------------------------------------------------------

        throwCancellationIfAborted(opts.signal);

        throw new Error("Did not receive response");
    }

    /**
     * Write any sort of data message to this channel. Returns a promise
     * that resolves when the write has completed
     */
    public async write(message: MessageData) {
        if (this.options.destination && !this.hasConnected) {
            // ensure we've "CONNECT"'d to the destination, if provided
            await this.socket.write({
                namespace: CONNECTION_NS,
                data: {
                    type: "CONNECT",
                    origin: {},
                },
                destination: this.options.destination,
            });
            this.hasConnected = true;
        }

        return this.socket.write({
            namespace: this.namespace,
            destination: this.options.destination,
            data: message,
        });
    }
}
