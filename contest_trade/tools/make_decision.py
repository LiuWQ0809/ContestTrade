from loguru import logger
from langchain_core.tools import tool

@tool(
    description="Make a decision under current given informations. You must make a decision when the budget is not enough to fetch more information.",
)
async def make_decision():
    logger.info("make_decision")
    return None
