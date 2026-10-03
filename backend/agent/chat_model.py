"""Chat Completions adapter retaining provider reasoning across tool turns.

LangChain's OpenAI client intentionally drops non-OpenAI message fields. These
conversion hooks preserve common extensions without coupling transport to one
vendor's SDK. The standard client still owns retries, tool parsing and usage.
"""

from __future__ import annotations

from copy import deepcopy

from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_openai.chat_models.base import BaseChatOpenAI


REASONING_FIELDS = (
    "reasoning_content", "reasoning", "reasoning_details", "encrypted_content",
)


def _reasoning_fields(payload: dict) -> dict:
    return {
        key: deepcopy(payload[key])
        for key in REASONING_FIELDS
        if payload.get(key) is not None
    }


class CompatibleChatModel(BaseChatOpenAI):
    """Use standard Chat Completions plus provider-returned reasoning fields.

    BaseChatOpenAI avoids ChatOpenAI's model-name-based role rewrites and its
    automatic max_tokens -> max_completion_tokens conversion, which need not
    be supported by another provider. Opaque reasoning is kept only for replay;
    the research service emits displayable text separately.
    """

    def _convert_chunk_to_generation_chunk(
        self, chunk, default_chunk_class, base_generation_info,
    ):
        result = super()._convert_chunk_to_generation_chunk(
            chunk, default_chunk_class, base_generation_info,
        )
        choices = chunk.get("choices") or []
        if result and choices and isinstance(result.message, AIMessageChunk):
            result.message.additional_kwargs.update(
                _reasoning_fields(choices[0].get("delta") or {})
            )
        return result

    def _create_chat_result(self, response, generation_info=None):
        result = super()._create_chat_result(response, generation_info)
        payload = response if isinstance(response, dict) else response.model_dump()
        for generation, choice in zip(result.generations, payload["choices"]):
            if isinstance(generation.message, AIMessage):
                generation.message.additional_kwargs.update(
                    _reasoning_fields(choice.get("message") or {})
                )
        return result

    def _get_request_payload(self, input_, *, stop=None, **kwargs):
        messages = self._convert_input(input_).to_messages()
        payload = super()._get_request_payload(messages, stop=stop, **kwargs)
        for message, serialized in zip(messages, payload["messages"]):
            if isinstance(message, AIMessage):
                serialized.update(_reasoning_fields(message.additional_kwargs))
        return payload
