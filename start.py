# ! /usr/bin/env python
# -*- coding: utf-8 -*-

import random
from time import sleep
import traceback
from datetime import datetime, date, time, timedelta
from pprint import pprint
import threading

import httplib2
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

import requests
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType

from firebase import firebase as fb

from access_data import group_token, group_id, firebase_url

CREDENTIALS_FILE = 'credentials.json'
spreadsheet_id = "17daTxQymgrqxxHhSOmrmAncAzvcYzvhbRmsa4DG-Q8M"

bells_schedule = [time(8, 0), time(9, 30), time(11, 10), time(13, 00), time(14, 40), time(16, 20), time(18, 10)]


class ScheduleChecker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        now = datetime.utcnow() + timedelta(hours=3)
        previous_minute = now.minute
        while True:
            now = datetime.utcnow() + timedelta(hours=3)
            if now.minute != previous_minute and now.minute % 5 != 0:
                try:
                    schedule_broadcast()
                except Exception as e:
                    logs('Error:\n' + traceback.format_exc())
                    sleep(2)
                    continue
                previous_minute = now.minute
                sleep(240)
            else:
                sleep(60)


def schedule_broadcast():
    table = get_table()
    now = datetime.utcnow() + timedelta(hours=3)
    for line in table:
        if len(line) > 3 and now.weekday() + 1 == int(line[0]) and line[3]:
            if not line[2]:
                begin = datetime.combine(date.today(), bells_schedule[int(line[1])-1])
            else:
                begin = datetime.combine(date.today(), datetime.strptime(line[2], '%H:%M').time())
                # print(begin - now)
            if timedelta(seconds=5*60) < begin - now < timedelta(seconds=10*60):
                message = generate_message(begin, line)
                bot_session = vk_api.VkApi(token=group_token)
                bot_api = bot_session.get_api()
                send_message(bot_api, get_user_list(), message)
            # print(line)


def generate_message(begin, line: list):
    lesson = 'занятие'
    if len(line) > 5:
        if line[5] == 'л':
            lesson = 'лекция'
        elif line[5] == 'п':
            lesson = 'практика'
    message = '%s < Сейчас начнется %s по предмету «%s»' % (begin.strftime('%H:%M'), lesson, line[3])
    platform, platform_index = '', 0
    if len(line) > 6:
        if line[6] == 'з':
            platform, platform_index = 'ZOOM', 1
        elif line[6] == 'м':
            platform, platform_index = 'MS Teams', 2
        elif line[6] == 'д':
            platform, platform_index, = 'Discord', 3
    if platform:
        message += ' в %s. ' % platform
    else:
        message += '. '
    if len(line) > 4 and line[4]:
        message += 'Преподаватель: %s. ' % line[4]
    if platform and len(line) > 7 and line[7]:
        message += 'Ссылка%s: %s. ' % (['', ' на конференцию', ' на команду', ' на канал'][platform_index], line[7])
    if len(line) == 9:
        message += 'Коментарий: ' + line[8]
    return message


def get_table():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build('sheets', 'v4', http=httpAuth)

    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='A2:I43',
        majorDimension='ROWS'
    ).execute()['values']
    # pprint(values)
    return values


def bot_activation():
    bot_session = vk_api.VkApi(token=group_token)
    bot_api = bot_session.get_api()
    while True:
        longpoll = VkBotLongPoll(bot_session, group_id)
        try:
            # print('wait')
            for event in longpoll.listen():
                # print('got event')
                if event.type == VkBotEventType.MESSAGE_NEW and event.from_user:
                    # print('message')
                    from_id = event.message['from_id']
                    message = event.message['text'].lower()
                    if message in ['начать', 'start']:
                        # print(event.message)
                        if from_id not in get_user_list():
                            add_user(from_id)
                            send_message(bot_api, [from_id],
                                         'Включил тебя в рассылку. Чтобы отписаться пришли «Отписаться».')
                        else:
                            send_message(bot_api, [from_id], 'Ты уже подписался на рассылку.')
                    elif message == 'отписаться':
                        user_ids = get_user_list()
                        if from_id in user_ids:
                            remove_user(from_id)
                            send_message(bot_api, [from_id], 'Ты больше не будешь получать от меня сообщения. '
                                                             'Если передумаешь, напиши мне «Начать».')
                        else:
                            send_message(bot_api, [from_id], 'Ты не был подписан на рассылку. '
                                                             'Подпишись, чтобы получать расписание от меня.')

        except requests.exceptions.ReadTimeout:
            sleep(10)
        except:
            exit()


def add_user(user_id):
    firebase = fb.FirebaseApplication(firebase_url)
    data = {
        'user_id': user_id
    }
    result = firebase.post('/pmi-1/user_ids', data)
    # print(result)


def full_users_data():
    firebase = fb.FirebaseApplication(firebase_url)
    result = firebase.get('/pmi-1/user_ids', '')
    return result


def get_user_list():
    user_id = []
    users_data = full_users_data()
    if users_data:
        for user in users_data.items():
            user_id.append(user[1]['user_id'])
    return user_id


def remove_user(user_id):
    firebase = fb.FirebaseApplication(firebase_url)
    users_data = full_users_data()
    recording = ''
    if users_data:
        for k, v in users_data.items():
            if user_id == v['user_id']:
                recording = k
        if recording:
            # print(recording)
            firebase.delete('/pmi-1/user_ids', recording)


def send_message(bot_api, user_ids, message):
    bot_api.messages.send(
        random_id=random.getrandbits(32),
        user_ids=user_ids,
        message=message
    )


def logs(s: str):
    with open('logs.txt', 'a+', encoding='utf8') as df:
        df.write(s + '\n')


if __name__ == '__main__':
    scheduleChecker = ScheduleChecker()
    scheduleChecker.start()
    while True:
        try:
            bot_activation()
        except Exception as e:
            logs('Error:\n' + traceback.format_exc())
            sleep(10)
