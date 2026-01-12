from loguru import logger
from langchain_core.tools import tool
from pydantic import BaseModel, Field

@tool(
    description="""Generate a final report to the user. The task can't continue when your call this tool. So make sure you have enough information to write a report."""
)
async def final_report():
    logger.info("final_report")
    return None
