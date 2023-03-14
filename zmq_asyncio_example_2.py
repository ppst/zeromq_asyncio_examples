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
        self.__create_work_loops()
        self.__make_workers()
        self.__set_event_loop()
        self.__stopped = False


    def __set_event_loop(self):
        self.__eventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.__eventLoop)
        print("Broker loop set")


    def start_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()


    def __create_work_loops(self):
        """Creates event loops to which work requests are submitted.

           Thanks to: https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
        """
        self.__workLoops = []
        for l in range(2):
            loop = asyncio.new_event_loop()
            t = threading.Thread(target = BrokerNode.start_loop, args=(loop, ), daemon=True)
            t.start()
            self.__workLoops.append(loop)


    def __make_workers(self):
        self.__worker = {}
        for w in range(10):
            wn = "worker." + str(w + 1)
            worker = WorkerClass(wn, self.__context)
            self.__worker[wn] = worker


    def __connect(self, context):
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

        for loop in self.__workLoops:
            loop.stop()
        self.__stopped = True
        self.__poller.unregister(self.__router)
        self.__poller.unregister(self.__results)
        self.__poller.unregister(self.__stopper)
        self.__router.close()
        self.__results.close()
        self.__stopper.close()
        print("Broker sockets closed")
        self.__eventLoop.stop()


    async def __route(self, msg):
        """Runs designated worker on one of the event loops.
        """
        source = msg[0]
        if msg[1] == b'':
            # Message came from a REQ socket
            js = json.loads(msg[2])
        else:
            js = json.loads(msg[1])
        worker = self.__worker[js["worker"]]
        js["source"] = source.decode()
        loop = self.__workLoops[random.randrange(2)]
        asyncio.run_coroutine_threadsafe(worker.route(js), loop)


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



class WorkerClass():

    def __init__(self, name, context):
        self.__name = name
        self.__context = context
        self.__req = 0

    async def __request(self, worker):
        """Sends request to another worker via broker
        """

        print(self.__name, "sending request")
        self.__req += 1
        sock = self.__context.socket(zmq.DEALER)
        sock.setsockopt_string(zmq.IDENTITY, self.__name + "." + str(self.__req) + ".request")
        sock.connect("ipc:///tmp/broker.ipc")
        await sock.send_json({"messageNo": self.__req, "workerRequest": self.__name, "worker": worker})
        res = await sock.recv_json()
        print("Received response:", res)
        sock.close()


    async def route(self, msg):
        """Replies to received message with a simple response.
        """

        print(self.__name, "received message:", msg)

        # Communication with other workers via broker
        if self.__name == "worker.1":
            await self.__request("worker.2")

        resultSock = self.__context.socket(zmq.DEALER)
        resultSock.setsockopt_string(zmq.IDENTITY, self.__name + "." + str(msg["messageNo"]))
        resultSock.connect("ipc:///tmp/results.ipc")
        msg["response"] = self.__name
        await resultSock.send_json(msg)
        resultSock.close()



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
        w = random.randrange(10) + 1
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
