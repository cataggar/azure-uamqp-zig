#!/usr/bin/env python3
"""A small AMQP 1.0 broker for the interop check, built on Apache Qpid Proton.

Proton is the reference C implementation of AMQP 1.0 and shares no code or
reading of the spec with this repository, which is the whole point: the Zig
tests all talk to a peer written from the same assumptions as the code under
test, and can agree with it while both are wrong.

Queues are created on demand. Nothing is persisted and nothing is secured;
this exists to answer "do real AMQP implementations understand our bytes".

    pip install python-qpid-proton
    ./interop/broker.py [listen-address]   # default 0.0.0.0:5672
"""

import collections
import sys

from proton.handlers import MessagingHandler
from proton.reactor import Container


class Broker(MessagingHandler):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.queues = collections.defaultdict(collections.deque)
        self.consumers = collections.defaultdict(list)

    def on_connection_bound(self, event):
        # PLAIN as well as ANONYMOUS, so the mechanism Azure services use is
        # exercised too. Any password is accepted: this is a test fixture.
        sasl = event.transport.sasl()
        sasl.allow_insecure_mechs = True
        sasl.allowed_mechs("ANONYMOUS PLAIN")

    def on_start(self, event):
        event.container.listen(self.url)
        print(f"broker listening on {self.url}", flush=True)

    @staticmethod
    def address_of(link):
        if link.is_sender:
            return link.remote_source.address
        return link.remote_target.address

    def on_link_opening(self, event):
        link = event.link
        address = self.address_of(link)
        if address is None:
            link.condition = "amqp:invalid-field"
            return
        if link.is_sender:
            link.source.address = address
            self.consumers[address].append(link)
            print(f"consumer attached to {address}", flush=True)
        else:
            link.target.address = address
            print(f"producer attached to {address}", flush=True)

    def on_link_closing(self, event):
        self.forget(event.link)

    def on_disconnected(self, event):
        for link in list(event.connection.links()) if event.connection else []:
            self.forget(link)

    def forget(self, link):
        if not link.is_sender:
            return
        address = self.address_of(link)
        if link in self.consumers[address]:
            self.consumers[address].remove(link)

    def on_message(self, event):
        address = self.address_of(event.link)
        self.queues[address].append(event.message)
        print(f"message received for {address}", flush=True)
        self.dispatch(address)

    def on_sendable(self, event):
        self.dispatch(self.address_of(event.link))

    def dispatch(self, address):
        queue = self.queues[address]
        while queue:
            ready = [c for c in self.consumers[address] if c.credit > 0]
            if not ready:
                return
            ready[0].send(queue.popleft())
            print(f"message delivered from {address}", flush=True)


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0:5672"
    try:
        Container(Broker(url)).run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
