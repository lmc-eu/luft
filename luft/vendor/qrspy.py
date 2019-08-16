"""This module helps to work with Qlik Sense Repository API (QRS API).

Original source from https://github.com/clintcarr/qrspy
"""
import json
import logging
import random
import string
from pathlib import Path

from luft.common.config import (QLIK_ENT_CLIENT_CERT, QLIK_ENT_CLIENT_KEY,
                                QLIK_ENT_HOST, QLIK_ENT_PORT, QLIK_ENT_ROOT_CERT)

import requests

requests.packages.urllib3.disable_warnings()


def get_qrs():
    """Return qlik app id list for apps with tag from DATA_LINEAGE_TAG variable."""
    try:
        qrs = QlikBase(
            server=f'{QLIK_ENT_HOST}:{QLIK_ENT_PORT}',
            certificate=(str(Path.cwd() / QLIK_ENT_CLIENT_CERT),
                         str(Path.cwd() / QLIK_ENT_CLIENT_KEY)),
            root=str(Path.cwd() / QLIK_ENT_ROOT_CERT))
        print('Connected to Qlik Sense.')
        return qrs
    except Exception:
        raise Exception('Qlik Sense Connect failed!')


class QlikBase:
    """
    Instantiates the Qlik Repository Service Class.

    Import this class.
    """

    def __init__(
        self,
        server,
        certificate=False,
        root=False
    ):
        """
        Establish connectivity with Qlik Sense Repository Service.

        :param server: servername.domain:4242
        :param certificate: path to client.pem and client_key.pem certificates
        :param root: path to root.pem certificate
        """
        adapter = requests.adapters.HTTPAdapter(max_retries=20)

        self.server = server
        self.certificate = certificate
        self.root = root
        self.session = requests.session()
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.xrf = self._set_xrf()
        self.headers = {
            'X-Qlik-XrfKey': self.xrf,
            'Accept': 'application/json',
            'X-Qlik-User': 'UserDirectory=Internal;UserID=sa_repository',
            'Content-Type': 'application/json',
            'Connection': 'Keep-Alive'}

    def _set_xrf(self):
        characters = string.ascii_letters + string.digits
        return ''.join(random.sample(characters, 16))

    def get(self, endpoint):
        """
        Perform GET method to Qlik Repository Service endpoints.

        :param endpoint: API endpoint path
        """
        if '?' in endpoint:
            response = self.session.get('https://{0}/{1}&xrfkey={2}'
                                        .format(
                                            self.server,
                                            endpoint,
                                            self.xrf),
                                        headers=self.headers,
                                        verify=False,
                                        cert=self.certificate)
            return response
        else:
            response = self.session.get('https://{0}/{1}?xrfkey={2}'
                                        .format(
                                            self.server,
                                            endpoint,
                                            self.xrf),
                                        headers=self.headers,
                                        verify=False,
                                        cert=self.certificate)
            return response

    def delete(self, endpoint):
        """
        Perform DELETE method to Qlik Repository Service endpoints.

        :param endpoint: API endpoint path
        """
        if '?' in endpoint:
            response = self.session.delete(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                           headers=self.headers, verify=self.root,
                                           cert=self.certificate)
            return response.status_code
        else:
            response = self.session.delete(f'https://{self.server}/{endpoint}?xrfkey={self.xrf}',
                                           headers=self.headers, verify=self.root,
                                           cert=self.certificate)
            return response.status_code

    def post(self, endpoint, data=None):
        """
        Perform POST method to Qlik Repository Service endpoint.

        :param endpoint: API endpoint path
        """
        # TODO: remove and not ... condition once we have moved from task_name running to airflow
        if '?' in endpoint and not str(endpoint).__contains__('qrs/task/start/synchronous?name'):
            if data is None:
                response = self.session.post(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                             headers=self.headers,
                                             verify=self.root, cert=self.certificate)
                return response.status_code
            else:
                response = self.session.post(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                             headers=self.headers, data=data,
                                             verify=self.root, cert=self.certificate)
                return response.status_code
        # TODO: remove this elif part of condition once we have moved
        # from task_name running to airflow
        elif str(endpoint).__contains__('qrs/task/start/synchronous?name'):
            if data is None:
                response = self.session.post(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                             headers=self.headers,
                                             verify=self.root, cert=self.certificate)
                return response.status_code, response.content
            else:
                response = self.session.post(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                             headers=self.headers, data=data,
                                             verify=self.root, cert=self.certificate)
                return response.status_code, response.content
        else:
            if data is None:
                response = self.session.post(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                             headers=self.headers,
                                             verify=self.root, cert=self.certificate)
                return response.status_code, response.content
            else:
                response = self.session.post(f'https://{self.server}/{endpoint}&xrfkey={self.xrf}',
                                             headers=self.headers, data=data,
                                             verify=self.root, cert=self.certificate)
                return response.status_code, response.content

    def start_task_id(self, task_id):
        """
        Start a task.

        :param task_id: Task id to start
        :returns: Task Execution ID
        """
        path = 'qrs/task/{0}/start/synchronous'.format(task_id)
        return self.post(path)

    def start_task_name(self, task_name):
        """
        Start a task.

        :param task_name: Task name to start
        :returns: Task Execution ID
        """
        path = 'qrs/task/start/synchronous?name={0}'.format(task_name)
        return self.post(path)

    def get_task_execution(self, exec_id):
        """
        Get task execution.

        :param exec_id: execution id
        :returns: JSON
        """
        path = 'qrs/executionresult?filter=executionID eq {0}'.format(exec_id)
        return json.loads(self.get(path).text)

    def get_task_execution_full(self):
        """
        Get task execution - full.

        :returns: JSON
        """
        path = 'qrs/executionresult/full'
        return json.loads(self.get(path).text)

    def get_exportappticket(self, appid):
        """
        Return the an app export ticket for use in export_app.

        :param appid: Application ID for export
        :returns: JSON
        """
        path = 'qrs/app/{0}/export'.format(appid)
        return json.loads(self.get(path).text)

    def get_app(self):
        """
        Return the applications.

        :param filterparam: Property and operator of the filter
        :param filtervalue: Value of the filter
        :returns: JSON
        """
        path = 'qrs/app'
        return json.loads(self.get(path).text)

    def export_app(self, appid, filepath, filename):
        """
        Export an application to the filepath.

        :param appid: application id to export
        :param filepath: location to store the exported app
        :param filename: name of file
        :returns: HTTP Status Code
        """
        exportticket = self.get_exportappticket(appid)
        logging.info('Export ticket generated')
        ticket = (exportticket['value'])
        path = 'qrs/download/app/{0}/{1}/{2}'.format(appid, ticket, filename)
        data = self.get(path).content
        Path(filepath).mkdir(exist_ok=True)
        with open(filepath + filename + '.qvf', 'wb') as file:
            file.write(data)
        return 'Application: {0} written to {1}'.format(filename, filepath)

    def export_app_content(self, appid, filename):
        """
        Export an application to the filepath.

        :param appid: application id to export
        :param filepath: location to store the exported app
        :param filename: name of file
        :returns: HTTP Status Code
        """
        exportticket = self.get_exportappticket(appid)
        logging.info('Export ticket generated')
        ticket = (exportticket['value'])
        path = 'qrs/download/app/{0}/{1}/{2}'.format(appid, ticket, filename)
        data = self.get(path).content

        return data

    def reload_app(self, appid):
        """
        Reload an application.

        :param appid: Application ID to reload
        :returns: HTTP Status Code
        """
        path = 'qrs/app/{0}/reload'.format(appid)
        return self.post(path)
