from vllm_base import get_llm

GPU_COUNT = 2
MODEL = "deepseek-ai/DeepSeek-R1-Distill-Llama-70B"
CONTEXT_LEN = 32768

llm, sampling_params = get_llm(GPU_COUNT, MODEL, CONTEXT_LEN)

def llm_gen(prompt):
    outputs = llm.generate([prompt], sampling_params)
    return [output.outputs[0].txt for output in outputs]
