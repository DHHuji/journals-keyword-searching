from vllm_base import get_llm

MODEL = "deepseek-ai/DeepSeek-R1-Distill-Llama-70B"
CONTEXT_LEN = 32768

def build_llm_gen(gpu_count):
    llm, sampling_params = get_llm(gpu_count, MODEL, CONTEXT_LEN)

    def llm_gen(prompts):
        outputs = llm.generate(prompts, sampling_params)
        return [output.outputs[0].txt for output in outputs]

    return llm_gen
