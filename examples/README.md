# Examples

Try these pipeline examples to get a better feel for TigerFlow (ordered from simple to complex):

| Pipeline                   | Description                                                                                     |
|----------------------------|-------------------------------------------------------------------------------------------------|
| `simple_pipeline_local`    | A pipeline of local tasks to download books and count the words in each.                        |
| `simple_pipeline_slurm`    | A pipeline combining Slurm and local tasks to download books, count words in each, and ingest the counts into a database. |
| `audio_feature_extraction` | A pipeline combining Slurm and local tasks to transcribe audio files, generate text embeddings, and ingest the embeddings into a database. |

Make sure to install the package with the additional dependencies required to run the examples:

```bash
pip install tigerflow[examples]
```
