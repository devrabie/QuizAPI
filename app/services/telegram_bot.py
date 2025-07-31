import aiohttp
import asyncio
import json

class TelegramBotServiceAsync :
    def __init__(self, token):
        self.token = token
        self.api_url = f"https://api.telegram.org/bot{token}/"

    async def _post(self, action, data):
        url = self.api_url + action
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as response:
                return await response.json()

    async def send_message(self, data: object):
        return await self._post("sendMessage", data)

    async def send(self, action, data: object):
        return await self._post(action, data)

    async def edit_message(self, data: object):
        return await self._post("editMessageText", data)

    async def send_photo(self, data: object):
        return await self._post("sendPhoto", data)

    async def send_video(self, data: object):
        return await self._post("sendVideo", data)

    async def send_document(self, data: object):
        return await self._post("sendDocument", data)

    async def send_audio(self, data: object):
        return await self._post("sendAudio", data)

    async def send_sticker(self, data: object):
        return await self._post("sendSticker", data)

    async def send_animation(self, data: object):
        return await self._post("sendAnimation", data)

    async def send_voice(self, data: object):
        return await self._post("sendVoice", data)

    async def delete_message(self, data: object):
        return await self._post("deleteMessage", data)
    async def delete_messages(self, data: object):
        return await self._post("deleteMessages", data)

    async def forward_message(self, data: object):
        return await self._post("forwardMessage", data)

    async def get_chat_member_count(self, data: object):
        return await self._post("getChatMemberCount", data)

    async def edit_message_reply_markup(self, data: object):
        return await self._post("editMessageReplyMarkup", data)

    async def get_chat_invite_link(self, chat_id):
        data = {
            'chat_id': chat_id
        }
        return await self._post("exportChatInviteLink", data)

    async def edit_inline_message(self, data: dict) -> dict:
        # The 'inline_message_id' is passed directly in the data dict
        # We also need to remove 'chat_id' and 'message_id' if they are present
        data_to_send = data.copy()
        data_to_send.pop('chat_id', None)
        data_to_send.pop('message_id', None)
        return await self._post("editMessageText", data_to_send)

    async def answer_callback_query(self, callback_query_id: str, text: str, show_alert: bool = False) -> dict:
        data = {
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": show_alert
        }
        return await self._post("answerCallbackQuery", data)