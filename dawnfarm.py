import asyncio
import random
import sys
from typing import List, Set

from loguru import logger
from loader import config, semaphore, file_operations
from core.bot import Bot
from models import Account
from utils import setup
from database import initialize_database


accounts_with_initial_delay: Set[str] = set()


async def run_module_safe(account: Account) -> None:
    global accounts_with_initial_delay

    async with semaphore:
        bot = Bot(account)
        try:
            if config.delay_before_start.min > 0 and account.email not in accounts_with_initial_delay:
                random_delay = random.randint(config.delay_before_start.min, config.delay_before_start.max)
                logger.info(f"Account: {account.email} | Initial farming delay: {random_delay} sec")
                await asyncio.sleep(random_delay)
                accounts_with_initial_delay.add(account.email)

            await bot.process_farming()
        finally:
            await bot.close_session()


async def farm_continuously(accounts: List[Account]) -> None:
    while True:
        random.shuffle(accounts)
        tasks = [run_module_safe(account) for account in accounts]
        await asyncio.gather(*tasks)
        await asyncio.sleep(10)


def reset_initial_delays():
    global accounts_with_initial_delay
    accounts_with_initial_delay.clear()


async def run() -> None:
    await initialize_database()
    await file_operations.setup_files()
    reset_initial_delays()

    accounts = config.accounts_to_farm

    if not accounts:
        logger.error("No accounts for farming")
        return

    await farm_continuously(accounts)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    setup()
    asyncio.run(run())
