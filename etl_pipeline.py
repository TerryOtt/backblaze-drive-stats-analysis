import typing


_pipeline_state: dict[str, list[str] | None | int] = {
    'stages'            : None,
    'curr_stage_idx'    : None,
}


def create_pipeline(stage_descriptions: typing.Iterable[str]) -> None:
    global _pipeline_state

    if _pipeline_state['stages'] is not None:
        print("WARN: repeat call to create_pipeline")

    _pipeline_state['stages']: list[str] = []

    for stage_description in stage_descriptions:
        _pipeline_state['stages'].append(stage_description)

    _pipeline_state['curr_stage_idx']: int = 1


def next_stage_banner() -> str:
    global _pipeline_state

    stage_banner: str = f"\nETL pipeline stage {_pipeline_state['curr_stage_idx']:2} of " \
                        f"{len(_pipeline_state['stages']):2}: " \
                        f"{_pipeline_state['stages'][_pipeline_state['curr_stage_idx'] - 1]}"

    _pipeline_state['curr_stage_idx'] += 1

    return stage_banner
