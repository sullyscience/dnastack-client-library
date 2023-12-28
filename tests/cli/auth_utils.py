import os.path

import re
from subprocess import PIPE, Popen
from sys import stderr
from time import sleep
from typing import List

from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from selenium.webdriver import Chrome
    from selenium.common.exceptions import JavascriptException, NoSuchElementException, TimeoutException
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
except ImportError:
    stderr.write('WARNING: The package "selenium" is not detected. This may break any tests that require selenium, '
                 'e.g., authentication tests.')

from dnastack.common.environments import env, flag
from dnastack.common.logger import get_logger


class UnexpectedCommandProcessTerminationError(RuntimeError):
    pass


class UnexpectedLoginError(RuntimeError):
    pass


def handle_device_code_flow(cmd: List[str], email: str, token: str) -> str:
    """ Handle the device code flow """
    logger = get_logger(f'{os.path.basename(__file__)}/handle_device_code_flow')
    re_confirmation_url = re.compile(r'https?://[^\s]+/authorize\?user_code=[^\s]+')

    logger.debug('Start handling the device code flow...')

    process_env = {
        e_k: e_v
        for e_k, e_v in os.environ.items()
        if e_k != 'DNASTACK_DEBUG'
    }

    p = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, env=process_env)
    device_code_url = None
    while device_code_url is None:
        exit_code = p.poll()
        if exit_code is not None:
            if exit_code == 0:
                logger.info(f'No further auth actions necessary')
                output = p.stdout.read()
                logger.info(f'CLI: EXIT: {exit_code}')
                logger.info(f'CLI: STDOUT: {output}')
                logger.info(f'CLI: STDERR: {p.stderr.read()}')
                p.stdout.close()
                p.stderr.close()
                return output
            else:
                logger.error(f'CLI: EXIT: {exit_code}')
                logger.error(f'CLI: STDOUT: {p.stdout.read()}')
                logger.error(f'CLI: STDERR: {p.stderr.read()}')
                p.stdout.close()
                p.stderr.close()

                raise UnexpectedCommandProcessTerminationError(exit_code)
        try:
            output = p.stderr.readline()
            matches = re_confirmation_url.search(output)

            logger.debug(f'OUTPUT READ: {output.encode()}')

            if matches:
                device_code_url = matches.group(0)
                logger.debug(f'Detected the device code URL ({device_code_url})')
            else:
                sleep(1)
        except KeyboardInterrupt:
            p.kill()
            raise RuntimeError('User terminated')

    logger.debug('Confirming the device code')
    confirm_device_code(device_code_url, email, token)
    logger.debug('Waiting for the CLI to join back...')

    while True:
        exit_code = p.poll()
        if exit_code is not None:
            break

    output = p.stdout.read()
    error_output = p.stderr.read()

    p.stdout.close()
    p.stderr.close()

    assert exit_code == 0, f'Unexpected exit code {exit_code}:\nSTDOUT:\n{output}\nERROR:\n{error_output}'

    logger.debug('Finished handling the device code flow...')

    return output


def _get_web_driver() -> WebDriver:
    inside_docker_container = bool(
        env('PYTHON_VERSION', required=False)
        and env('PYTHON_SETUPTOOLS_VERSION', required=False)
        and env('PYTHON_PIP_VERSION', required=False)
    )

    asked_for_headless_mode = flag('E2E_HEADLESS')
    use_headless_mode = inside_docker_container or asked_for_headless_mode

    chrome_options = Options()
    if use_headless_mode:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    return Chrome(options=chrome_options)


def _login_via_personal_access_token(driver: WebDriver, email: str, token: str):
    from tests.exam_helper import WithTestUserTestCase, wallet_base_uri
    if WithTestUserTestCase.test_user_prefix in email:
        driver.get(f'{wallet_base_uri}/login')
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, "//a[contains(@href, 'login')]"))
    )
    driver.execute_script(
        f"document.querySelector('form[name=\"token\"] input[name=\"token\"]').value = '{token}';"
        f"document.querySelector('form[name=\"token\"] input[name=\"email\"]').value = '{email}';"
    )
    token_form = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "form[name='token']"))
    )
    token_form.submit()


def confirm_device_code(device_code_url: str, email: str, token: str):
    logger = get_logger(f'{__file__}/confirm_device_code')

    assert device_code_url, '"device_code_url" is undefined'
    assert email, '"email" is undefined'
    assert token, '"token" is undefined'

    driver = _get_web_driver()

    if 'user_code=' not in device_code_url:
        logger.error('Device code url must have "user_code" query param', device_code_url)
        raise RuntimeError('Invalid device code url')

    try:
        # Get user code from the device code url
        user_code = device_code_url[device_code_url.index('=') + 1:]
        # Load URL in browser
        driver.get(device_code_url)
        # Assert login page is loaded
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//a[contains(@href, 'login')]")),
            message='Expecting login page'
        )
        # Login via personal access token
        _login_via_personal_access_token(driver=driver, email=email, token=token)
        # Assert authorize device page is loaded
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "authorization")),
            message='Expecting authorization page'
        )
        # Fill user code in device authorization page
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@name='user_code']"))
        ).send_keys(user_code)
        # Click on continue button
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "continue-btn"))
        ).click()
        # Assert device confirmation page is loaded
        try:
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, "confirmation")),
                message='Expecting device confirmation page'
            )
        except Exception as e:
            logger.error("Error detecting while confirming the device.")
            logger.error(f"The source code of the current page ({driver.current_url}):\n{driver.page_source}")
            driver.quit()
            raise e
        # Click on allow button
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "allow-btn"))
        ).click()
        # Assert device authorization success page is loaded
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "authorization-success")),
            message='Expecting device authorization success page'
        )
    except Exception as e:
        logger.error(f'Failed to confirm the device due to {e}')
        raise RuntimeError('Failed to confirm the device code.') from e
    finally:
        driver.quit()
