import asyncio
import os

import aiofiles
import aiohttp

from tigerflow.tasks import LocalAsyncTask


class Embed(LocalAsyncTask):
    @staticmethod
    async def setup(context):
        context.url = "https://api.voyageai.com/v1/embeddings"
        context.headers = {
            "Authorization": f"Bearer {os.environ['VOYAGE_API_KEY']}",
            "Content-Type": "application/json",
        }
        context.session = aiohttp.ClientSession()
        print("Session created successfully!")

    @staticmethod
    async def run(context, input_file, output_file):
        async with aiofiles.open(input_file, "r") as f:
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
    async def teardown(context):
        await context.session.close()
        print("Session closed successfully!")


Embed.cli()
