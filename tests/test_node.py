from fluid import node


async def test_node_worker() -> None:
    worker = node.NodeWorker()
    assert worker.name() == "node_worker"
    assert worker.uid == worker.uid
