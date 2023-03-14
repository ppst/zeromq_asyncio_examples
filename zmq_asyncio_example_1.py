# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import asyncio
import json
import random
import threading
import time
import zmq
import zmq.asyncio


class BrokerNode(threading.Thread):
    """The broker mediates messages between main client and worker nodes. It also starts and stops
       workers.
    """

    def __init__(self, context):
        super().__init__()
        self.__connect(context)
        self.__make_workers()
        self.__set_event_loop()
        self.__stopped = False


    def __set_event_loop(self):
        self.__eventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.__eventLoop)
        print("Broker loop set")


    def __make_workers(self):
        """Creates workers with one Push and one Pair socket per worker.
        """

        self.__workers = {}
        for w in range(4):
            wname = "worker." + str(w + 1)
            wsock = self.__context.socket(zmq.PUSH)
            wsock.bind("ipc:///tmp/" + wname + ".ipc")
            wstop = self.__context.socket(zmq.PAIR)
            wstop.bind("ipc:///tmp/" + wname + ".stop.ipc")
            worker = WorkerNode(wname, self.__context)
            worker.start()
            self.__workers[wname] = {"work": wsock, "stop": wstop, "worker": worker}


    def __connect(self, context = None):
        """Sets up ØMQ sockets.
        """

        self.__context = context
        self.__router = self.__context.socket(zmq.ROUTER)
        self.__router.bind("ipc:///tmp/broker.ipc")
        self.__results = self.__context.socket(zmq.ROUTER)
        self.__results.bind("ipc:///tmp/results.ipc")
        self.__stopper = self.__context.socket(zmq.PAIR)
        self.__stopper.bind("ipc:///tmp/stopper.ipc")
        self.__poller = zmq.asyncio.Poller()
        self.__poller.register(self.__router, zmq.POLLIN)
        self.__poller.register(self.__results, zmq.POLLIN)
        self.__poller.register(self.__stopper, zmq.POLLIN)


    async def __stop(self):
        """Shuts down workers, unregisters and closes ØMQ sockets, and stops event loop. This
           method is not called directly, but invoked by signal via the stop socket.
        """

        self.__stopped = True
        for w in self.__workers:
            self.__workers[w]["stop"].send(b'')
            self.__workers[w]["stop"].close()
            self.__workers[w]["work"].close()
        self.__poller.unregister(self.__router)
        self.__poller.unregister(self.__results)
        self.__poller.unregister(self.__stopper)
        self.__router.close()
        self.__results.close()
        self.__stopper.close()
        print("Broker sockets closed")
        self.__eventLoop.stop()


    async def __route(self, msg):
        """Forwards request to designated worker.
        """

        source = msg[0]
        if msg[1] == b'':
            # Message came from a REQ socket
            js = json.loads(msg[2])
        else:
            js = json.loads(msg[1])
        worker = self.__workers[js["worker"]]
        js["source"] = source.decode()
        await worker["work"].send_json(js)


    async def __respond(self, msg):
        """Returns response to request source.
        """

        source = msg[0]
        js = json.loads(msg[1])
        js["responseSocket"] = source.decode()
        await self.__router.send_multipart([js["source"].encode(), json.dumps(js).encode()])


    async def __inner_run(self):
        """Routes requests and responses and listens for stop signal.
        """

        socks = dict(await self.__poller.poll())

        # Stop signal
        if socks.get(self.__stopper) == zmq.POLLIN:
            await self.__stop()
            return

        # Work request that needs to be routed to worker
        if socks.get(self.__router) == zmq.POLLIN:
            msg = await self.__router.recv_multipart()
            await self.__route(msg)

        # Result that needs to be returned to original sender
        if socks.get(self.__results) == zmq.POLLIN:
            msg = await self.__results.recv_multipart()
            await self.__respond(msg)


    def run(self):
        while not self.__stopped:
            self.__eventLoop.run_until_complete(self.__inner_run())



class WorkerNode(threading.Thread):
    """The worker node receives messages from the broker and responds.
    """

    def __init__(self, name, context):
        super().__init__()
        self.__name = name
        self.__connect(context)
        self.__set_event_loop()
        self.__stopped = False

    def __connect(self, context = None):
        """Connects worker to sockets bound by broker.
        """

        self.__context = context
        self.__work = self.__context.socket(zmq.PULL)
        self.__work.connect("ipc:///tmp/" + self.__name + ".ipc")
        self.__results = self.__context.socket(zmq.DEALER)
        self.__results.setsockopt_string(zmq.IDENTITY, self.__name)
        self.__results.connect("ipc:///tmp/results.ipc")
        self.__stopper = self.__context.socket(zmq.PAIR)
        self.__stopper.connect("ipc:///tmp/" + self.__name + ".stop.ipc")
        self.__poller = zmq.asyncio.Poller()
        self.__poller.register(self.__work, zmq.POLLIN)
        self.__poller.register(self.__stopper, zmq.POLLIN)


    def __set_event_loop(self):
        self.__eventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.__eventLoop)
        print(self.__name + " loop set")


    async def __stop(self):
        """Unregisters and disconnects ØMQ sockets and stops event loop. This
           method is not called directly, but invoked by signal via the stop socket.
        """
        self.__stopped = True
        self.__poller.unregister(self.__work)
        self.__poller.unregister(self.__stopper)
        self.__work.close()
        self.__results.close()
        self.__stopper.close()
        print(self.__name + " sockets closed")
        self.__eventLoop.stop()


    async def __request(self, worker):
        """Sends request to another worker node via broker
        """
        print(self.__name, "sending request")
        sock = self.__context.socket(zmq.DEALER)
        sock.setsockopt_string(zmq.IDENTITY, self.__name + ".request")
        sock.connect("ipc:///tmp/broker.ipc")
        await sock.send_json({"workerRequest": self.__name, "worker": worker})
        res = await sock.recv_json()
        print("Received response:", res)
        sock.close()


    async def __route(self):
        """Replies to received message with a simple response.
        """

        # Receives request message
        msg = await self.__work.recv_json()
        msg["response"] = self.__name
        print(self.__name," received:", msg)

        # Communication with other workers via broker
        if self.__name == "worker.4":
            await self.__request("worker.1")
        elif self.__name == "worker.2":
            await self.__request("worker.3")

        # Sends reply
        self.__results.send_json(msg)


    async def __inner_run(self):
        """Listens for requests and stop signal and runs on the event loop
           associated with this thread.
        """

        socks = dict(await self.__poller.poll())

        # Stop signal
        if socks.get(self.__stopper) == zmq.POLLIN:
            await self.__stop()
            return

        # Work request
        if socks.get(self.__work) == zmq.POLLIN:
            asyncio.run_coroutine_threadsafe(self.__route(), self.__eventLoop)


    def run(self):
        while not self.__stopped:
            self.__eventLoop.run_until_complete(self.__inner_run())



async def main():
    """Async main method that starts up a broker and sends some messages.
    """

    context = zmq.asyncio.Context()
    broker = BrokerNode(context)
    time.sleep(0.01)
    broker.start()
    socket  = context.socket(zmq.DEALER)
    socket.setsockopt_string(zmq.IDENTITY, "main_request_socket")
    socket.connect("ipc:///tmp/broker.ipc")
    stopper  = context.socket(zmq.PAIR)
    stopper.connect("ipc:///tmp/stopper.ipc")

    for n in range(100):
        print("Message", n + 1)
        w = random.randrange(4) + 1
        await socket.send_json({"messageNo": n + 1, "worker": "worker." + str(w)})
        res = await socket.recv_json()
        print("Response:", res)

    stopper.send(b'')
    socket.close()
    stopper.close()
    time.sleep(0.01)
    context.destroy()


if __name__ == '__main__':
    asyncio.run(main())
