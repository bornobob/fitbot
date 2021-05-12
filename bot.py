from discord.ext import commands
from database import Database
from datetime import datetime
from leagueapi import LeagueAPI
from json import loads


SETTINGS_JSON = './settings.json'


with open(SETTINGS_JSON, 'r') as f:
    settings = loads(f.read())


bot = commands.Bot('!')
database = Database(settings['database'])
api = LeagueAPI(settings['riot_token'])


@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')


@bot.command()
async def join(ctx):
    author = ctx.author
    author_id = author.id
    if database.member_exists(author_id):
        await ctx.send(f'{author.mention}, you already joined in the past!')
    else:
        database.add_member(author_id)
        await ctx.send(f'{author.mention}, you joined the fitbot community!')


@bot.command()
async def stop(ctx):
    author = ctx.author
    permissions = author.permissions_in(ctx.channel)
    if permissions.administrator:
        await bot.close()
        print('Stopped bot')
    else:
        await ctx.send('You do not have the permissions to stop me')


@bot.command()
async def done(ctx, amount: int):
    author = ctx.author
    author_id = author.id
    if database.member_exists(author_id):
        database.add_pushups_done(author_id, amount)
        done = database.pushups_done(author_id, datetime.now())
        net_amount = database.get_net_status(author_id)
        net_str = f' This brings your balance to {net_amount} push-ups!'
        if amount > 0:
            await ctx.send(f'{author.mention}, nice job, you have done ' + 
                           f'{done} push-ups today!' + net_str)
        else:
            await ctx.send(f'{author.mention}, if that is really what you ' +
                           'call an achievement... Your counter for today ' +
                           f'now is: {done} push-ups' + net_str)
    else:
        await ctx.send(f'{author.mention}, you have not joined the community' +
                       ' yet')


@bot.command()
async def sync(ctx):
    author = ctx.author
    author_id = author.id
    if not database.member_exists(author_id):
        await ctx.send(f'{author.mention}, you have not joined the community' +
                       ' yet')
    elif not database.member_has_paired_account(author_id):
        await ctx.send(f'{author.mention}, you do not have a paired account ' +
                       'yet')
    else:
        paired_acc = database.get_paired_account(author_id)
        last_sync = database.last_sync_date(author_id)
        result = api.get_deaths_by_date(paired_acc, last_sync)
        deaths, new_sync_date, limited = result[0], result[1], result[2]
        total_deaths = 0
        if deaths:
            for date, dths in deaths.items():
                total_deaths += dths
                database.add_pushups_todo(author_id, dths * 5, date)
        database.save_sync_date(author_id, new_sync_date)
        new_net = database.get_net_status(author_id)
        net_str = f' Your new balance is: {new_net} push-ups!'
        limited_str = ''
        if limited:
            limited_str = ' Warning: I got rate-limited by the API, please' + \
                          ' sync again later for better synchronization.'
        await ctx.send(f'{author.mention}, did some syncing, added ' + 
                       f'{total_deaths} deaths' + net_str + limited_str)


@bot.command()
async def status(ctx):
    author = ctx.author
    author_id = author.id
    if database.member_exists(author_id):
        net_amount = database.get_net_status(author_id)
        net_str = f'the standings are... {net_amount} push-ups!'
        await ctx.send(f'{author.mention}, {net_str}')
    else:
        await ctx.send(f'{author.mention}, you have not joined the community' +
                       ' yet')


@bot.command()
async def todo(ctx, amount: int):
    author = ctx.author
    author_id = author.id
    if database.member_exists(author_id):
        today = datetime.utcnow()
        database.add_pushups_todo(author_id, amount, today)
        net_amount = database.get_net_status(author_id)
        net_str = f'your new total is {net_amount} push-ups!'
        await ctx.send(f'{author.mention}, {net_str}')
    else:
        await ctx.send(f'{author.mention}, you have not joined the community' +
                       ' yet')


@bot.command()
async def pair(ctx, summoner_name: str):
    author = ctx.author
    author_id = author.id
    if database.member_exists(author_id):
        if not database.member_has_paired_account(author_id):
            acc_id = api.find_account_id(summoner_name)
            if acc_id:
                if database.account_is_paired(acc_id):
                    await ctx.send(f'{author.mention}, that account is ' +
                                   'already paired')
                else:
                    database.pair_account(author_id, acc_id)
                    await ctx.send(f'{author.mention}, pairing succesful!')
            else:
                await ctx.send(f'{author.mention}, account could not be ' + 
                               'connected (not found using riot api)')
        else:
            await ctx.send(f'{author.mention}, connecting multiple accounts ' +
                           'is currently unsupported') 
    else:
        await ctx.send(f'{author.mention}, you have not joined the community' +
                       ' yet')


bot.run(settings['discord_token'])
