# -*- coding: utf-8 -*-
"""Qlik Cloud Task."""

import time
import urllib.parse
from pathlib import Path
from typing import Dict, List

from luft.common.config import (
    QLIK_CLOUD_DELAY, QLIK_CLOUD_HEADLESS, QLIK_CLOUD_LOGIN_URL, QLIK_CLOUD_PASS, QLIK_CLOUD_URL,
    QLIK_CLOUD_USER)
from luft.common.logger import setup_logger
from luft.common.utils import NoneStr
from luft.tasks.generic_task import GenericTask
from luft.vendor.qrspy import get_qrs

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as exp
from selenium.webdriver.support.ui import Select, WebDriverWait

# Setup logger
logger = setup_logger('common', 'INFO')


class QlikCloud(GenericTask):
    """Qlik Sense Load App Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 account_id: str, apps: List[Dict[str, str]],
                 yaml_file: NoneStr = None, env: NoneStr = None,
                 thread_name: NoneStr = None, color: NoneStr = None):
        """Initialize BigQuery JDBC Task.

        Attributes:
        name (str): name of task.
        task_type (str): type of task. E.g. embulk-jdbc-load, mongo-load, etc.
        source_system (str): name of source system. Usually name of database.
            Used for better organization especially on blob storage. E.g. jobs, prace, pzr.
        source_subsystem (str): name of source subsystem. Usually name of schema.
            Used for better organization especially on blob storage. E.g. public, b2b.
        account_id (str): Qlik sense cloud account id.
        apps List[Dict[str, str]]: List of apps parameters.
        yaml_file (str): yaml filepath.
        env (str): environment - PROD, DEV.
        thread_name (str): name of thread for Airflow parallelization.
        color (str): hex code of color. Airflow operator will have this color.

        """
        self.account_id = account_id
        self.apps = apps

        options = Options()  # Chrome options
        options.headless = QLIK_CLOUD_HEADLESS  # Run headless
        self.browser = webdriver.Chrome(chrome_options=options)  # Browser

        super().__init__(name=name, task_type=task_type,
                         source_system=source_system,
                         source_subsystem=source_subsystem,
                         yaml_file=yaml_file,
                         env=env, thread_name=thread_name, color=color)

    def __call__(self, ts: str, env: NoneStr = None):
        """Make class callable.

        Attributes:
            ts (str): time of valid.

        """
        self.update_apps()

    def get_env_vars(self, ts: str, env: NoneStr = None) -> Dict[str, str]:
        """Get Docker enviromental variables."""
        env_dict = super().get_env_vars(ts=ts, env=env)
        clean_dict = self.clean_dictionary(env_dict)
        return clean_dict

    def _login(self):
        """Login into Qlik Sense Cloud."""
        logger.info('Logging into Qlik Sense Cloud.')
        self.browser.get(QLIK_CLOUD_LOGIN_URL)
        # Wait until form will show
        try:
            user_input = WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.ID, 'MemberLoginForm_LoginForm_QUsername')))
        except TimeoutException:
            logger.error('Logging has timeouted.')
        # Find elements
        pass_input = self.browser.find_element_by_id(
            'MemberLoginForm_LoginForm_qPassword')
        submit_btn = self.browser.find_element_by_class_name('qlik-submit')
        logger.info('Writting credentials.')
        user_input.send_keys(QLIK_CLOUD_USER)  # Write username
        pass_input.send_keys(QLIK_CLOUD_PASS)  # Write password
        submit_btn.click()  # Do login
        self._wait()  # Necessary
        logger.info('Logged into Qlik Sense Cloud.')

    def _wait(self, wait_time: float = 3):
        """Wait.

        Parameters:
        wait_time (int)

        """
        time.sleep(wait_time)

    def _main_site(self):
        """Go to main site of Qlik Sense Cloud."""
        self.browser.get(QLIK_CLOUD_URL)
        logger.info('Going to main site.')
        # Wait till it renders
        try:
            WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.CLASS_NAME, 'header__logo')))
            logger.info('Site has rendered!')
        except TimeoutException:
            logger.error('Main site has timeouted.')

    def _find_apps(self, search_str) -> List[WebElement]:
        """Find and return apps passing search string.

        Parameters:
        search_str (string): name of application to find.

        """
        obj_list = self.browser.find_elements_by_xpath(
            f'//div[@title="{search_str}"]')
        return obj_list

    def _check_apps(self, search_list: List[str]):
        """Check if applications are in Qlik Sense Cloud.

        Parameters:
        search_list (List[str]): list of applications to check.

        """
        for search_str in search_list:
            objects = self._find_apps(search_str)
            if len(objects) < 1:
                raise ValueError(f'App {search_str} is cannot be found.')
            else:
                logger.info(f'App {search_str} is loaded and ok.')

    def _right_click_app(self, app: WebElement):
        """Perform right click on Qlik application.

        Parameters:
        app (WebElement): application element to perform the click.

        """
        action = ActionChains(self.browser)
        action.context_click(app).perform()

    def _remove_app(self, app: WebElement):
        """Remove Qlik Sense Cloud App.

        Parameters:
        app (WebElement): application element to be removed.

        """
        self._right_click_app(app)
        self.browser.find_element_by_id('remove').click()
        self.browser.find_element_by_id('delete').click()

    def _switch_account(self, account_id: str):
        """Switch Qlik Sense Account.

        Parameters:
        account_id (str): identifier of Qlik Sense Cloud account.

        """
        # Get URL
        url_suffix = f'/hub/groups/{self.account_id}'
        url = urllib.parse.urljoin(QLIK_CLOUD_URL, url_suffix)
        self.browser.get(url)
        # Wait till it renders
        try:
            WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.CLASS_NAME, 'header__logo')))
            logger.info(f'Switched to account: {self.account_id}')
        except TimeoutException:
            logger.error(
                f'Switching to account {self.account_id} timeouted.')

    def _wait_all_upload(self, wait_time: int = 60):
        """Wait till all data in loading are loaded.

        Parameters:
        wait_time (int): seconds to wait till every app is loaded.

        """
        # Wait until uploaded files will show
        try:
            WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.CLASS_NAME, 'uploaded-files')))
        except TimeoutException:
            logger.error('Waiting timouted')
        # Get list of uploded files
        uploaded_files = self.browser.find_elements_by_xpath(
            '//div[contains(@class, "uploaded-files")]')
        for uploaded_file in uploaded_files:
            # Get progress bar
            progress_bar = uploaded_file.find_element_by_class_name(
                'progress-bar')
            # Get loded application name
            object_name = uploaded_file.find_element_by_class_name('file-name')
            # Wait till wait time
            t_end = time.time() + wait_time
            while time.time() < t_end:
                self._wait(0.5)
                # get progress percentage
                progress = progress_bar.get_attribute(
                    'style').replace('width: ', '').replace(';', '')
                logger.info(f'{object_name.text} loading: {progress}')
                if progress == '100%':
                    break

    def _upload_apps(self, app_path_list: List[Path]):
        """Upload application from local filesystem into Qlik Cloud.

        Parameters:
        app_path_list(List[Path]): list of application paths to upload.

        """
        try:
            # Get and click new app button
            new_app = WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.CLASS_NAME, 'app-upload--container')))
            new_app.click()
            # Get and click upload app button
            upload_app = WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.ID, 'upload-app')))
            upload_app.click()
            self._wait()  # needed
            # get form for uploading apps
            input_form = WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.ID, 'upload-input')))
            # Add path to input form
            app_path_str = list(map(lambda x: str(x), app_path_list))
            input_form.send_keys('\n'.join(app_path_str))
            self._wait()  # needed
            # Wait untill all apps are loaded
            self._wait_all_upload()
            # Get and click done-upload button
            done_upload = WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                exp.presence_of_element_located((By.ID, 'done-upload')))
            done_upload.click()
        except Exception:
            raise

    def _export_app_qse(self, apps: List[Dict[str, str]]) -> List[Path]:
        """Export Qlik App(its content) in binary format from Qlik Sense Enterprise(QSE).

        Parameters:
        apps (List[Dict[str, str]]): List of apps parameters.

        """
        qrs = get_qrs()
        filepath = './qlik_export/'  # Qlik temporary folder
        app_files = []

        for app in apps:
            filename = app.get('filename')
            app_id = app.get('qse_id')
            try:
                logger.info(f'Exporting Qlik App: {filename}')

                t0 = time.time()
                qrs.export_app(appid=app_id, filepath=filepath,
                               filename=filename)
                t1 = time.time()
                total = round((t1 - t0) / 60, 2)
                logger.info(f'App {filename}, id: {app_id} was exported sucessfully. '
                            f'Exporting time: {total}')
                app_files.append(Path.cwd() / filepath / f'{filename}.qvf')
            except Exception:
                raise
        return app_files

    def _remove_apps(self, app_name_list: List[str]):
        """Remove applications.

        Parameters:
        app_name_list List[str]: list of appication names to remove.

        """
        self._wait(1)
        for app_name in app_name_list:
            i = 1
            # Search for all instances of certain app
            while True:
                self._wait(1)
                app_list = self._find_apps(app_name)
                if len(app_list) > 0:
                    logger.info(f'Removing app {app_name} - {i}.')
                    self._remove_app(app_list[0])
                    i += 1
                else:
                    break

    def _publish_apps(self, apps: List[Dict[str, str]]):
        """Publish applications to stream."""
        for app_def in apps:
            # Get application webelement
            self._wait(1)
            app_name = app_def.get('name')
            apps_publish = self._find_apps(app_name)
            if len(apps_publish) != 1:
                raise ValueError(
                    f'There is multiple instances of {app_name}'
                    f'[{len(apps_publish)}].')
            app = apps_publish[0]
            # Right click on app and select publish
            self._right_click_app(app)
            self.browser.find_element_by_id('publish').click()
            try:
                WebDriverWait(self.browser, int(QLIK_CLOUD_DELAY)).until(
                    exp.presence_of_element_located((By.XPATH,
                                                     '//select[@ng-model="publishToStreamId"]')))
            except TimeoutException:
                logger.error('Waiting timouted')
            # Select the right stream
            select_stream = Select(
                self.browser.find_element_by_xpath('//select[@ng-model="publishToStreamId"]'))
            select_stream.select_by_visible_text(app_def.get('qsc_stream'))
            self._wait(1)
            # Confirm publishing
            logger.info(f'Publishing app {app_name}.')
            self.browser.find_element_by_id('publish-group-app').click()
            logger.info(f'App {app_name} is published.')
            self._wait(4)

    def update_apps(self):
        """Export, delete old app, upload new one and publish an app."""
        logger.info(f'Updating applications.')
        t0 = time.time()
        app_name_list = list(
            map(lambda x: x.get('name'), self.apps))
        app_files = self._export_app_qse(self.apps)
        # export apps to local filesystem
        self._login()
        self._main_site()
        self._switch_account(self.account_id)
        self._remove_apps(app_name_list)
        self._upload_apps(app_files)
        self._check_apps(app_name_list)
        self._publish_apps(self.apps)
        t1 = time.time()
        total = round((t1 - t0), 2)
        logger.info(f'It took {total} secs.')
