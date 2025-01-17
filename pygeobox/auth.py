###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import click
import logging
import requests
from secrets import token_hex

from pygeobox import cli_helpers
from pygeobox.env import AUTH_URL

LOGGER = logging.getLogger(__name__)


def create_token(topic: str, token: str) -> bool:
    """
    Creates a token with access control

    :param topic: `str` topic hierarchy
    :param token: `str` authentication token

    :returns: `bool` of result
    """
    data = {'topic': topic, 'token': token}

    r = requests.post(f'{AUTH_URL}/add_token', data=data)
    LOGGER.info(r.json().get('description'))

    return r.ok


def delete_token(topic: str, token: str = '') -> bool:
    """
    Creates a token with access control

    :param topic: `str` topic hierarchy
    :param token: `str` authentication token

    :returns: `bool` of result
    """
    data = {'topic': topic}

    if token != '':
        # Delete all tokens for a given th
        data['token'] = token

    r = requests.post(f'{AUTH_URL}/remove_token', data=data)
    click.echo(r.json().get('description'))

    return r.ok


def is_resource_open(topic: str) -> bool:
    """
    Checks if topic has access control configured

    :param topic: `str` topic hierarchy

    :returns: `bool` of result
    """
    headers = {'X-Original-URI': topic}

    r = requests.get(f'{AUTH_URL}/authorize', headers=headers)

    return r.ok


def is_token_authorized(topic: str, token: str) -> bool:
    """
    Checks if token is authorized to access a topic

    :param topic: `str` topic hierarchy
    :param token: `str` authentication token

    :returns: `bool` of result
    """
    headers = {
        'X-Original-URI': topic,
        'Authorization': f'Bearer {token}',
    }

    r = requests.get(f'{AUTH_URL}/authorize', headers=headers)

    return r.ok


@click.group()
def auth():
    """Auth workflow"""
    pass


@click.command()
@click.pass_context
@cli_helpers.OPTION_PATH
def is_restricted(ctx, path):
    """Check if topic has access control"""
    click.echo(not is_resource_open(path))


@click.command()
@click.pass_context
@cli_helpers.OPTION_PATH
@click.argument('token')
def has_access(ctx, path, token):
    """Check if a token has access to a topic"""
    if path is None:
        raise click.ClickException('Missing path or topic hierarchy')


    click.echo(is_token_authorized(path, token))


@click.command()
@click.pass_context
@cli_helpers.OPTION_PATH
@click.option('--yes', '-y', default=False, is_flag=True, help='Automatic yes')
@click.argument('token', required=False)
def add_token(ctx, path, yes, token):
    """Add access token for a topic"""

    token = token_hex(32) if token is None else token
    if yes:
        click.echo(f'Using token: {token}')
    elif not click.confirm(f'Continue with token: {token}', prompt_suffix='?'):
        return

    if create_token(path, token):
        click.echo('Token successfully created')


@click.command()
@click.pass_context
@cli_helpers.OPTION_PATH
@click.argument('token', required=False, nargs=-1)
def remove_token(ctx, path, token):
    """Delete one to many tokens for a topic"""

    if path is None:
        raise click.ClickException('Missing path or topic hierarchy')

    if delete_token(path, token):
        click.echo('Token successfully deleted')


auth.add_command(add_token)
auth.add_command(remove_token)
auth.add_command(has_access)
auth.add_command(is_restricted)
