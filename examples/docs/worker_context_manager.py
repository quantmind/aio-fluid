import asyncio

import dotenv
from pydantic_ai import Agent

from fluid.utils.worker import QueueConsumer

ARTICLES = [
    (
        "Async programming in Python allows multiple tasks to run concurrently "
        "within a single thread by yielding control at await points, which is "
        "particularly efficient for I/O-bound workloads such as HTTP requests "
        "and database queries."
    ),
    (
        "Large language models are trained on vast corpora of text using "
        "self-supervised objectives, enabling them to learn grammar, facts, and "
        "reasoning patterns that can be adapted to a wide range of downstream "
        "tasks through prompting or fine-tuning."
    ),
    (
        "The observer pattern decouples event producers from consumers by "
        "introducing an intermediary that maintains a list of subscribers and "
        "notifies them when state changes, making it straightforward to add new "
        "listeners without modifying the source."
    ),
]


class AiAgent(QueueConsumer[str]):

    async def run(self) -> None:
        agent = Agent(
            "google-gla:gemini-3-pro-preview",
            instructions=(
                "Your task is to summarise "
                "text in one short sentence."
            ),
        )
        while not self.is_stopping():
            print(self.is_stopping())
            item = await self.get_message()
            if item is None:
                continue
            if item == "":
                break
            print(f"Summarize: {item}")
            #result = await agent.run(item)
            #print(result.output)
            print(self.is_stopping())


async def main() -> None:
    async with AiAgent() as worker:
        for article in ARTICLES:
            worker.send(article)
        worker.send("")


if __name__ == "__main__":
    dotenv.load_dotenv()
    asyncio.run(main())
