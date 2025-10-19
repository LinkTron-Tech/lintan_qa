import logging
from abc import ABC, abstractmethod

# import gevent
# from gevent import spawn
import asyncio
from ..node.abstract_node import AbstractNode

logger = logging.getLogger(__name__)


class NodeGroup(AbstractNode, ABC):
    def __init__(self, all_nodes):
        super().__init__()
        if len(all_nodes) > 0:
            self.head = all_nodes[0]
            self.tail = all_nodes[-1]
        assert len(all_nodes) != 0, f"No node to compose the node group {self.__name__}"
        self.all_nodes = {node.__name__: node for node in all_nodes}
        self._connect_nodes()
        # self._watch_nodes = gevent.spawn(self._watch_nodes)
        self._watch_task = asyncio.create_task(self._watch_nodes())

    @abstractmethod
    def _connect_nodes(self):
        logger.error(
            f"Not implemented the self._connect_nodes method in {self.__name__}"
        )

    async def _watch_nodes(self):
        # 每隔 5 s 检查一次：只要所有子节点 is_start 都变为 False，就把组级 is_start=False，并退出自身
        while self.is_start:
            if all([not node.is_start for node in self.all_nodes.values()]):
                self.is_start = False
            await asyncio.sleep(5)

    def start(self):
        # 命名方式示例图
        # graph TD
        # subgraph MyGroup["serial_number = [0]"]
        #     A["Node A\nserial_number=[0,0]"]
        #     B["Node B\nserial_number=[0,1]"]
        #     C["Node C\nserial_number=[0,2]"]
        # end

        # X["Source X"] --> A
        # Y["Source Y"] --> B
        # A --> C
        # B --> C
        if self.serial_number is None:
            self.serial_number = [0]
        tasks = []
        for i, node in enumerate(self.all_nodes.values()):
            node.set_serial_number(self.serial_number + [i])
            tasks.extend(node.start())
        self.is_start = True
        return tasks

    async def end(self):
        # 修复没有外部数据流入的节点，无法结束的问题
        # 例如：只有一个pipeline
        heads = self._find_head_nodes() or [self.head]
        for head in heads:
            await head.end()
        await self._watch_task
        self.is_start = False


    ##找到一组节点的头节点
    # 上级节点不和自己在一个组的节点就是头节点，否则是组内的节点
    # 或者组内自己就是头节点前面没有节点的节点
    # 可能有多个数据流向，所以会有多个头节点
    # graph LR
    # %% 定义组内节点 A、B、C
    # subgraph MyGroup
    #     A[Node A]
    #     B[Node B]
    #     C[Node C]
    # end

    # %% 组外数据来源 X、Y
    # X[Source X] --> A
    # Y[Source Y] --> B

    # %% 组内流向
    # A --> C
    # B --> C
    def _find_head_nodes(self):
        nodes = []
        for node in self.all_nodes.values():
            if any([src not in self.all_nodes.values() for src in node.src_nodes.values()]):
                nodes.append(node)
        return nodes
