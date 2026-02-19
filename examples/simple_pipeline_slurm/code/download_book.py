import asyncio
from pathlib import Path

from tigerflow.tasks import LocalAsyncTask
from tigerflow.utils import SetupContext


class DownloadBook(LocalAsyncTask):
    @staticmethod
    async def setup(context: SetupContext):
        import aiohttp

        context.session = aiohttp.ClientSession()
        print("Session created successfully!")

    @staticmethod
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        import aiofiles

        book_id = input_file.stem
        async with context.session.get(
            f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
        ) as resp:
            await asyncio.sleep(5)  # Simulate long-running request
            result = await resp.text()

        async with aiofiles.open(output_file, "w") as f:
            await f.write(result)

    @staticmethod
    async def teardown(context: SetupContext):
        await context.session.close()
        print("Session closed successfully!")


DownloadBook.cli()
