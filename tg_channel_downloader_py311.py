#!/usr/bin/env python3
import difflib
import os
import re
import time
import asyncio
import logging
from telethon import TelegramClient, events, errors
from telethon.tl.types import MessageMediaWebPage

# ***********************************************************************************#
api_id = 1000000   # your telegram api id
api_hash = '000000000000000000000000000000'  # your telegram api hash
bot_token = '0000000000:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'  # your bot_token
admin_id = 1000000  # your chat id
save_path = '/root/TG_Channel'  # file save path
upload_file_set = True  # set upload file to google drive
drive_id = ''  # google teamdrive id
drive_name = 'DriveName'  # rclone drive name
max_num = 5  # 同时下载数量
# filter file name/文件名过滤
filter_list = ['你好，欢迎加入', '\n']
# filter chat id /过滤某些频道不下载
blacklist = []
donwload_all_chat = True # 监控所有你加入的频道，收到的新消息如果包含媒体都会下载，默认关闭
filter_file_name = []
# ***********************************************************************************#

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.WARNING)
logger = logging.getLogger(__name__)
queue = asyncio.Queue()


# 文件夹/文件名称处理
def validateTitle(title):
    r_str = r"[\/\\\:\*\?\"\<\>\|\n]"  # '/ \ : * ? " < > |'
    new_title = re.sub(r_str, "_", title)  # 替换为下划线
    new_title = new_title.replace('：', '_')  # 中文冒号也替换为下划线
    return new_title


# 安全文件名，限制长度，不超过 Telegram 限制
def safe_filename(filename, limit=100):
    ext = os.path.splitext(filename)[1]  # .pdf, .jpg
    name = os.path.splitext(filename)[0]  # 文件名主体

    if len(filename) <= limit:
        return filename
    else:
        keep_len = limit - len(ext) - 3  # 3 是 '...'
        if keep_len <= 0:
            return '...' + ext
        half = keep_len // 2
        return name[:half] + '...' + name[-(keep_len - half):] + ext


# 压缩连续下划线
def normalize_underscores(filename):
    return re.sub(r'_+', '_', filename)


# 删除文件名中的空格
def remove_spaces(filename):
    return filename.replace(" ", "")


# 获取相册标题
async def get_group_caption(message):
    group_caption = ""
    entity = await client.get_entity(message.to_id)
    async for msg in client.iter_messages(entity=entity, reverse=True, offset_id=message.id - 9, limit=10):
        if msg.grouped_id == message.grouped_id:
            if msg.text != "":
                group_caption = msg.text
                return group_caption
    return group_caption


# 获取本地时间
def get_local_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# 判断相似率
def get_equal_rate(str1, str2):
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()


# 返回文件大小
def bytes_to_string(byte_count):
    suffix_index = 0
    while byte_count >= 1024:
        byte_count /= 1024
        suffix_index += 1

    return '{:.2f}{}'.format(
        byte_count, [' bytes', 'KB', 'MB', 'GB', 'TB'][suffix_index]
    )


async def worker(name):
    while True:
        queue_item = await queue.get()
        message = queue_item[0]
        chat_title = queue_item[1]
        entity = queue_item[2]
        file_name = queue_item[3]

        for filter_file in filter_file_name:
            if file_name.endswith(filter_file):
                queue.task_done()
                return

        file_name = safe_filename(file_name)
        file_name = normalize_underscores(file_name)
        file_name = remove_spaces(file_name)
        dirname = safe_filename(validateTitle(f'{chat_title}({entity.id})'), limit=50)
        datetime_dir_name = message.date.strftime("%Y%m")
        file_save_path = os.path.join(save_path, dirname, datetime_dir_name)

        if not os.path.exists(file_save_path):
            os.makedirs(file_save_path, exist_ok=True)

        if file_name in os.listdir(file_save_path):
            os.remove(os.path.join(file_save_path, file_name))

        print(f"{get_local_time()} 开始下载： {chat_title} - {file_name}")

        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(client.download_media(
                message, os.path.join(file_save_path, file_name)))
            await asyncio.wait_for(task, timeout=3600)

            if upload_file_set:
                proc = await asyncio.create_subprocess_exec(
                    'rclone',
                    'move',
                    os.path.join(file_save_path, file_name),
                    f"{drive_name}/{dirname}/{datetime_dir_name}",
                    '--ignore-existing',
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE
                )
                await proc.wait()
                if proc.returncode == 0:
                    print(f"{get_local_time()} - {file_name} 下载并上传完成")
                else:
                    logging.warning(f"{get_local_time()} - rclone move failed for {file_name}")

        except (errors.FileReferenceExpiredError, asyncio.TimeoutError):
            logging.warning(f'{get_local_time()} - {file_name} 出现异常，重新尝试下载！')
            async for new_message in client.iter_messages(entity=entity, offset_id=message.id - 1, reverse=True, limit=1):
                await queue.put((new_message, chat_title, entity, file_name))

        except Exception as e:
            print(f"{get_local_time()} - {file_name} {e}")
            await bot.send_message(admin_id, f'Error!\n\n{e}\n\n{file_name}')

        finally:
            queue.task_done()
            if upload_file_set:
                try:
                    os.remove(os.path.join(file_save_path, file_name))
                except Exception:
                    pass


# ======================== 事件处理 ============================
@events.register(events.NewMessage(pattern='/start', from_users=admin_id))
async def handler(update):
    text = update.message.text.split(' ')
    if len(text) == 1:
        await bot.send_message(admin_id, '参数错误，请按照参考格式输入:\n\n '
                                         '<i>/start https://t.me/fkdhlg 0 </i>\n\n'
                                         'Tips:如果不输入offset_id，默认从第一条开始下载。', parse_mode='HTML')
        return
    elif len(text) == 2:
        chat_id = text[1]
        try:
            entity = await client.get_entity(chat_id)
            chat_title = entity.title
            offset_id = 0
            await update.reply(f'开始从{chat_title}的第一条消息下载。')
        except Exception:
            await update.reply('chat输入错误，请输入频道或群组的链接')
            return
    elif len(text) == 3:
        chat_id = text[1]
        offset_id = int(text[2])
        try:
            entity = await client.get_entity(chat_id)
            chat_title = entity.title
            await update.reply(f'开始从{chat_title}的第{offset_id}条消息下载。')
        except Exception:
            await update.reply('chat输入错误，请输入频道或群组的链接')
            return
    else:
        await bot.send_message(admin_id, '参数错误，请按照参考格式输入:\n\n '
                                         '<i>/start https://t.me/fkdhlg 0 </i>\n\n'
                                         'Tips:如果不输入offset_id，默认从第一条开始下载。', parse_mode='HTML')
        return

    if chat_title:
        print(f'{get_local_time()} - 开始下载：{chat_title}({entity.id}) - {offset_id}')
        last_msg_id = 0
        async for message in client.iter_messages(entity, offset_id=offset_id, reverse=True, limit=None):
            if message.media:
                caption = await get_group_caption(message) if (
                    message.grouped_id and message.text == "") else message.text
                if len(filter_list) and caption != "":
                    for filter_keyword in filter_list:
                        caption = caption.replace(filter_keyword, "")
                caption = "" if caption == "" else validateTitle(caption)[:50] + ' - '
                file_name = ''
                if message.document:
                    if message.media.document.mime_type in ["image/webp", "application/x-tgsticker"]:
                        continue
                    for i in message.document.attributes:
                        try:
                            file_name = i.file_name
                        except Exception:
                            continue
                    if file_name == '':
                        file_name = f'{message.id} - {caption}.{message.document.mime_type.split("/")[-1]}'
                    else:
                        if get_equal_rate(caption, file_name) > 0.6:
                            caption = ""
                        file_name = f'{message.id} - {caption}{file_name}'
                elif message.photo:
                    file_name = f'{message.id} - {caption}{message.photo.id}.jpg'
                else:
                    continue
                await queue.put((message, chat_title, entity, file_name))
                last_msg_id = message.id
        await bot.send_message(admin_id, f'{chat_title} all message added to task queue, last message is：{last_msg_id}')


@events.register(events.NewMessage())
async def all_chat_download(update):
    message = update.message
    if message.media:
        chat_id = message.to_id
        entity = await client.get_entity(chat_id)
        if entity.id in blacklist:
            return
        chat_title = entity.title
        caption = await get_group_caption(message) if (
            message.grouped_id and message.text == "") else message.text
        if caption != "":
            for fw in filter_list:
                caption = caption.replace(fw, '')
        caption = "" if caption == "" else validateTitle(caption)[:50] + ' - '
        file_name = ''
        if message.document:
            try:
                if isinstance(message.media, MessageMediaWebPage):
                    return
                if message.media.document.mime_type in ["image/webp", "application/x-tgsticker"]:
                    file_name = f'{message.media.document.id}.{message.media.document.mime_type.split("/")[-1]}'
                for i in message.document.attributes:
                    try:
                        file_name = i.file_name
                    except Exception:
                        continue
                if file_name == '':
                    file_name = f'{message.id} - {caption}.{message.document.mime_type.split("/")[-1]}'
                else:
                    if get_equal_rate(caption, file_name) > 0.6:
                        caption = ""
                    file_name = f'{message.id} - {caption}{file_name}'
            except Exception:
                print(message.media)
        elif message.photo:
            file_name = f'{message.id} - {caption}{message.photo.id}.jpg'
        else:
            return

        file_name = safe_filename(file_name)
        file_name = normalize_underscores(file_name)
        file_name = remove_spaces(file_name)
        for filter_keyword in filter_list:
            file_name = file_name.replace(filter_keyword, "")

        print(chat_title, file_name)
        await queue.put((message, chat_title, entity, file_name))


# ======================== 主程序 ============================
async def main():
    bot.add_event_handler(handler)
    if donwload_all_chat:
        client.add_event_handler(all_chat_download)

    for i in range(max_num):
        asyncio.create_task(worker(f'worker-{i}'))

    print('Successfully started (Press Ctrl+C to stop)')
    await client.run_until_disconnected()
    print('Stopped!')


if __name__ == '__main__':
    bot = TelegramClient('telegram_channel_downloader_bot', api_id, api_hash)
    bot.start(bot_token=bot_token)

    client = TelegramClient('telegram_channel_downloader', api_id, api_hash)
    client.start()

    asyncio.run(main())
