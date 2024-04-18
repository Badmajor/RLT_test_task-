import asyncio
from aiogram import Bot, Dispatcher, F, types

from config import TOKEN
from utils import build_answer

bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.message(F.text)
async def request(message: types.Message):
    answer = await build_answer(message.text)
    await message.answer(answer)


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
