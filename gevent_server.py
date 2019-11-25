# coding:utf-8
# @author: cicada
# @email: 1713856662a@gmail.com
# @project: thriftPieces
# @file: gevent_server.py
# @date_time: 2019/11/23 上午12:24

from gevent import queue, monkey
monkey.patch_socket()
import logging

from gevent.pool import Pool
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.server.TServer import TServer
from thrift.transport import TTransport

logger = logging.getLogger(__name__)


class TGeventPoolServer(TServer):

    def __init__(self, greenlets, *args):
        TServer.__init__(self, *args)
        self.clients = queue.Queue()
        self.pool = Pool(size=greenlets)
        self.max_greenlets = greenlets

    def serve(self):
        self.serverTransport.listen()
        while True:
            while len(self.clients) > 0:
                for client in self.clients:
                    if len(self.pool) < self.max_greenlets:
                        self.pool.apply_async(func=self.serveClient, args=(client, ))
            client = self.serverTransport.accept()
            if not client:
                continue
            if len(self.pool) < self.max_greenlets:
                self.pool.apply_async(func=self.serveClient, args=(client, ))
            else:
                self.clients.put(client)

    def serveClient(self, client):
        itrans = self.inputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)

        if isinstance(self.inputProtocolFactory, THeaderProtocolFactory):
            otrans = None
            oprot = iprot
        else:
            otrans = self.outputTransportFactory.getTransport(client)
            oprot = self.outputProtocolFactory.getProtocol(otrans)

        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except Exception as x:
            logger.exception(x)

        itrans.close()
        if otrans:
            otrans.close()