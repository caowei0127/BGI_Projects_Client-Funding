import pandas as pd
import sys
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.orm import sessionmaker
from pandas import DataFrame
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, FLOAT, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.sql import func
import datetime
import smtplib
import requests
import json
import schedule
import time
import pygsheets


Base = declarative_base()

not_check_groups = ['A_ATG_R']


class MT4_User(Base):
    __tablename__ = 'mt4_users'

    login = Column(Integer, primary_key=True)
    balance = Column(FLOAT)
    credit = Column(FLOAT)
    leverage = Column(Integer)
    group = Column(String)


class MT4_Trade(Base):
    __tablename__ = 'mt4_trades'

    login = Column(Integer, primary_key=True)
    profit = Column(FLOAT)
    close_time = Column(DateTime)


def _get_account_details_():
    wks = _connect_to_google_drive_()
    account_details = DataFrame(wks.get_all_records())
    #account_details = pd.read_csv('20180207 AccountDetailsCurrent.csv')
    print(account_details)
    return account_details


def _get_account_equity_(account_details):
    account_details_copy = account_details.copy()
    live_list = ['1', '2', '3', '5']
    df_mt4_users = DataFrame(
        columns=['login', 'balance', 'credit', 'leverage', 'live'])
    df_mt4_trades = DataFrame(columns=['login', 'live', 'profit'])
    df_mt4_logins = DataFrame(columns=['group', 'login', 'live'])

    for live in live_list:
        engine = create_engine(
            'mysql+pymysql://prime:sL5F29su@192.168.100.73:3306/live' + live)
        # 创建数据表，如果数据表存在，则忽视        #Base.metadata.create_all(engine)
        Sessinon = sessionmaker(bind=engine)
        session = Sessinon()
        group_list = [str(group) for group in account_details_copy[account_details_copy['live'] == int(
            live)]['group'].drop_duplicates().values]
        group_list = [group for group in group_list if group[0:1] == '1']
        print(group_list)
        login_list = [int(value) for value in account_details_copy[account_details_copy['live'] == int(
            live)]['login'].values]
        mt4_users = session.query(MT4_User.login, MT4_User.balance, MT4_User.credit, MT4_User.leverage).filter(
            MT4_User.login.in_(login_list)).all()
        mt4_trades = session.query(MT4_Trade.login, MT4_Trade.profit).filter(
            MT4_Trade.login.in_(login_list)).filter(MT4_Trade.close_time == '1970-01-01 00:00:00').all()
        mt4_group_logins = session.query(MT4_User.group, MT4_User.login).filter(
            MT4_User.group.in_(group_list)).all()
        session.close()

        for mt4_user in mt4_users:
            df_mt4_users.loc[len(df_mt4_users)] = [int(
                mt4_user.login), float(mt4_user.balance), float(mt4_user.credit), int(mt4_user.leverage / 100), int(live)]
        for mt4_trade in mt4_trades:
            df_mt4_trades.loc[len(df_mt4_trades)] = [int(
                mt4_trade.login), int(live), float(mt4_trade.profit)]
        for mt4_group_login in mt4_group_logins:
            df_mt4_logins.loc[len(df_mt4_logins)] = [
                mt4_group_login.group, int(mt4_group_login.login), int(live)]

    df_mt4_trades_sum = df_mt4_trades.groupby(
        ['login', 'live'], as_index=False).sum()
    df_account_equity = pd.merge(df_mt4_users, df_mt4_trades_sum, on=[
                                 'login', 'live'], how='left').fillna(0)
    print('account equity: \n', df_account_equity, '\n')
    print('mt4 logins: \n', df_mt4_logins, '\n')
    have_new_login = _update_new_logins_(df_mt4_logins, account_details_copy)
    return df_account_equity, have_new_login


def _connect_to_google_drive_():
    gc = pygsheets.authorize()
    sh = gc.open_by_url(
        'https://docs.google.com/spreadsheets/d/1SQ2_9lc_98V5cVnKzV6aYbuNJPW6EIBSxX-VWekH_AA/edit#gid=1531828915')
    return sh.sheet1


def _update_new_logins_(df_mt4_logins, account_details_copy):
    df_account_details_old = account_details_copy[account_details_copy['group'].str.startswith(
        '1')]
    df_account_details_new = pd.merge(df_account_details_old, df_mt4_logins, on=[
        'login', 'live', 'group'], how='right')
    print('account details new: \n', df_account_details_new, '\n')
    df_new_logins = pd.concat([
        account_details_copy[account_details_copy['group'].str.startswith('1') == False], df_account_details_new])
    print('new logins: \n', df_new_logins, '\n')

    wks = _connect_to_google_drive_()
    rows = wks.rows - 1
    wks.delete_rows(2, rows)
    wks.insert_rows(
        row=1, number=df_new_logins.shape[1], values=np.array(df_new_logins.fillna('')).tolist(), inherit=True)
    return (rows < df_new_logins.shape[1])


def _process_all_info_(df_all_info, have_new_login):
    if have_new_login == True:
        df_all_info = df_all_info[df_all_info['exchangeRate'] != '']
    df_all_info['equity'] = (df_all_info['balance'] + df_all_info['credit'] +
                             df_all_info['profit']) * df_all_info['exchangeRate'] * df_all_info['leverage']
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    time = now.strftime('%H:00')
    df_all_info['date'] = date
    df_all_info['time'] = time
    print(df_all_info)

    lp_funding = _get_lp_funding_(_get_access_token_())
    df_lp_funding = DataFrame(columns=['date', 'time', 'lp_funding'])
    df_lp_funding.loc[0] = [date, time, lp_funding]

    return df_all_info, df_lp_funding


def _save_info_(df_all_info, df_lp_funding):
    engine = create_engine(
        'postgresql://postgres:12345@localhost:5432/Client Funding')
    df_all_info.to_sql("All Account Info", engine,
                       index=False, if_exists='append')
    df_lp_funding.to_sql("LP Funding", engine,
                         index=False, if_exists='append')


def _get_access_token_():
    url = "https://38.76.4.235:44300/api/token"
    headers = {'content-type': 'application/x-www-form-urlencoded',
               'grant_type': 'password', 'username': 'APITest',
               'password': 'cw12345..'}
    request = requests.post(url, data=headers, verify=False)
    data = request.json()
    return data['access_token']


def _get_lp_funding_(access_token):
    mc_lp = {10: 'LMAX', 11: 'Divisa', 22: 'Vantage'}
    lp_funding = 0.0
    for margin_account_number in mc_lp.keys():
        url_lp_funding = 'https://38.76.4.235:44300/api/rest/margin-account/' + \
            str(margin_account_number)
        request_equity = requests.get(url_lp_funding, headers={
            'Authorization': 'Bearer ' + access_token}, verify=False)
        data_equity = json.loads(request_equity.text)
        lp_funding += float(data_equity['equity'])
    return lp_funding


def main(argv=None):
    if argv is None:
        argv = sys.argv
    df_account_details = _get_account_details_()
    df_account_equity, have_new_login = _get_account_equity_(
        df_account_details)
    df_all_info = pd.merge(df_account_details, df_account_equity, on=[
        'login', 'live'])
    print('all info: \n', df_all_info, '\n')
    df_all_info, df_lp_funding = _process_all_info_(
        df_all_info.copy(), have_new_login)
    _save_info_(df_all_info, df_lp_funding)


if __name__ == "__main__":
    schedule.every(2).hours.do(main)
    #main()


while True:
    schedule.run_pending()
    time.sleep(1)