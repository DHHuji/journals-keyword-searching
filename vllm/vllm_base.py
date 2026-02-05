from vllm import LLM, SamplingParams

sampling_params = SamplingParams(
    temperature=0.3,
    top_p=0.9,
    max_tokens=4096,
    repetition_penalty=1.1
)

def get_llm(gpu_count, model, context_len):
    return LLM(
        model=model,
        tensor_parallel_size=gpu_count,
        trust_remote_code=True,
        max_model_len=context_len,
    ), sampling_params
