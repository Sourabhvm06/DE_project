"""Airflow DAG for ingesting Spotify recently-played events."""

import base64
import json
import os
from datetime import timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task


SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"


def _request_json(request: Request) -> dict:
	"""Perform an HTTPS request and return its JSON response."""
	try:
		with urlopen(request, timeout=30) as response:
			return json.loads(response.read().decode("utf-8"))
	except HTTPError as error:
		body = error.read().decode("utf-8", errors="replace")
		raise RuntimeError(f"Spotify request failed ({error.code}): {body}") from error


@dag(
	dag_id="spotify_recently_played_ingest",
	schedule="0 * * * *",
	start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
	catchup=False,
	default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
	tags=["ingest", "spotify", "music"],
)
def spotify_recently_played_ingest():
	"""Extract the previous hour of Spotify playback activity into raw JSON."""

	@task
	def fetch_recently_played() -> dict:
		client_id = os.environ["SPOTIFY_CLIENT_ID"]
		client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]
		refresh_token = os.environ["SPOTIFY_REFRESH_TOKEN"]

		credentials = base64.b64encode(
			f"{client_id}:{client_secret}".encode("utf-8")
		).decode("ascii")
		token_request = Request(
			SPOTIFY_TOKEN_URL,
			data=urlencode(
				{"grant_type": "refresh_token", "refresh_token": refresh_token}
			).encode("ascii"),
			headers={
				"Authorization": f"Basic {credentials}",
				"Content-Type": "application/x-www-form-urlencoded",
			},
			method="POST",
		)
		access_token = _request_json(token_request)["access_token"]

		query = urlencode({"limit": 50})
		data_request = Request(
			f"{SPOTIFY_RECENTLY_PLAYED_URL}?{query}",
			headers={"Authorization": f"Bearer {access_token}"},
		)
		return _request_json(data_request)

	@task
	def write_raw_response(payload: dict) -> str:
		execution_date = pendulum.now("UTC")
		output_root = Path(os.environ.get("SPOTIFY_RAW_ROOT", "/tmp/music_raw"))
		output_dir = output_root / (
			f"spotify/recently_played/year={execution_date.year}/"
			f"month={execution_date.month:02d}/day={execution_date.day:02d}/"
			f"hour={execution_date.hour:02d}"
		)
		output_dir.mkdir(parents=True, exist_ok=True)
		output_path = output_dir / f"run_{execution_date.format('YYYYMMDDTHHmmss')}Z.json"
		output_path.write_text(json.dumps(payload), encoding="utf-8")
		return str(output_path)

	write_raw_response(fetch_recently_played())


spotify_recently_played_ingest()
