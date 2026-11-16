"""Optional LangChain adapter; the embedding callable is supplied by the app."""
from dataclasses import asdict


def as_runnable(retriever, embed, model_version, **search_options):
    from langchain_core.documents import Document
    from langchain_core.runnables import RunnableLambda

    def retrieve(query):
        result = retriever.search(query, embed(query), model_version, **search_options)
        return [Document(page_content=candidate.body,
                         metadata={key: value for key, value in asdict(candidate).items() if key != "body"})
                for candidate in result.candidates]

    return RunnableLambda(retrieve)
