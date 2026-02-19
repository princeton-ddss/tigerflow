import asyncio
from pathlib import Path

from tigerflow.tasks import LocalAsyncTask
from tigerflow.utils import SetupContext


class Embed(LocalAsyncTask):
    @staticmethod
    async def setup(context: SetupContext):
        import os

        import aiohttp

        context.url = "https://api.voyageai.com/v1/embeddings"
        context.headers = {
            "Authorization": f"Bearer {os.environ['VOYAGE_API_KEY']}",
            "Content-Type": "application/json",
        }
        context.session = aiohttp.ClientSession()
        print("Session created successfully!")

    @staticmethod
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        import aiofiles

        async with aiofiles.open(input_file) as f:
            text = await f.read()

        async with context.session.post(
            context.url,
            headers=context.headers,
            json={
                "input": text.strip(),
                "model": "voyage-3.5",
                "input_type": "document",
            },
        ) as resp:
            resp.raise_for_status()  # Raise error if unsuccessful
            result = await resp.text()  # Raw JSON
            await asyncio.sleep(1)  # For API rate limit

        async with aiofiles.open(output_file, "w") as f:
            await f.write(result)

    @staticmethod
    async def teardown(context: SetupContext):
        await context.session.close()
        print("Session closed successfully!")


Embed.cli()
