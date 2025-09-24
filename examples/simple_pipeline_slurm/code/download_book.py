import asyncio

import aiofiles
import aiohttp

from tigerflow.tasks import LocalAsyncTask


class DownloadBook(LocalAsyncTask):
    @staticmethod
    async def setup(context):
        context.session = aiohttp.ClientSession()
        print("Session created successfully!")

    @staticmethod
    async def run(context, input_file, output_file):
        book_id = input_file.stem
        async with context.session.get(
            f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
        ) as resp:
            await asyncio.sleep(5)  # Simulate long-running request
            result = await resp.text()

        async with aiofiles.open(output_file, "w") as f:
            await f.write(result)

    @staticmethod
    async def teardown(context):
        await context.session.close()
        print("Session closed successfully!")


DownloadBook.cli()
