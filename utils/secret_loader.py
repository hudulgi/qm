import os
import atexit
import tempfile
import logging

logger = logging.getLogger(__name__)

_temp_files: list[str] = []


def _cleanup_temp_files():
    for path in _temp_files:
        try:
            os.unlink(path)
        except OSError:
            pass


atexit.register(_cleanup_temp_files)


def resolve_secret(secret_ref: str) -> str:
    """KIS 시크릿 파일 경로를 반환한다.

    환경변수 GCP_PROJECT_ID가 설정되어 있으면 Secret Manager에서
    시크릿을 가져와 임시 파일로 저장한 뒤 그 경로를 반환한다.
    설정되지 않으면 secret_ref를 그대로 반환한다(로컬 파일 모드).

    Args:
        secret_ref: 로컬 시크릿 파일 경로 또는 GCP 시크릿 이름.

    Returns:
        PyKis에 전달할 시크릿 파일 경로.
    """
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        return secret_ref

    # GCP 모드: 패키지를 여기서만 import
    try:
        from google.cloud import secretmanager
    except ImportError:
        raise ImportError(
            "google-cloud-secret-manager 패키지가 필요합니다. "
            "pip install google-cloud-secret-manager"
        )

    secret_id = secret_ref.replace(".json", "")
    name = f"projects/{project}/secrets/{secret_id}/versions/latest"

    logger.info(f"GCP Secret Manager에서 시크릿 로드: {secret_id} (project={project})")
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("utf-8")

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, prefix="kis_secret_"
    )
    tmp.write(payload)
    tmp.close()
    _temp_files.append(tmp.name)

    logger.info(f"시크릿을 임시 파일에 저장: {tmp.name}")
    return tmp.name