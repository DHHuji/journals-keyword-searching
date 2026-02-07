from vllm_base import get_llm

MODEL = "meta-llama/Meta-Llama-3.1-70B-Instruct"
CONTEXT_LEN = 32768

def build_llm_gen(gpu_count):
    llm, sampling_params = get_llm(gpu_count, MODEL, CONTEXT_LEN)

    def llm_gen(prompts):
        outputs = llm.generate(prompts, sampling_params)
        return [output.outputs[0].text for output in outputs]

    return llm_gen
