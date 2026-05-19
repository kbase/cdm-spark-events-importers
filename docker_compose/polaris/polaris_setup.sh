#!/bin/sh
set -e

POLARIS_HOST="${POLARIS_HOST:-http://polaris:8181}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-test-events}"
WAREHOUSE_PREFIX="${WAREHOUSE_PREFIX:-users-sql-warehouse}"
POLARIS_CATALOGS="${POLARIS_CATALOGS:-event_processor_startup_test event_processor_integration_test some_user}"
POLARIS_SERVICE_PRINCIPAL="${POLARIS_SERVICE_PRINCIPAL:-cdm_spark_events}"
POLARIS_SERVICE_PRINCIPAL_ROLE="${POLARIS_SERVICE_PRINCIPAL_ROLE:-service_admin}"
POLARIS_SERVICE_CREDENTIAL_FILE="${POLARIS_SERVICE_CREDENTIAL_FILE:-/shared-polaris/polaris_credential.txt}"

MGMT="${POLARIS_HOST}/api/management/v1"

echo "Getting OAuth token from Polaris..."
TOKEN_RESPONSE=$(curl -sf -X POST "${POLARIS_HOST}/api/catalog/v1/oauth/tokens" \
  -u "${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")

TOKEN="${TOKEN_RESPONSE#*\"access_token\":\"}"
TOKEN="${TOKEN%%\"*}"

if [ -z "${TOKEN}" ] || [ "${TOKEN}" = "${TOKEN_RESPONSE}" ]; then
  echo "ERROR: Failed to get OAuth token. Response: ${TOKEN_RESPONSE}"
  exit 1
fi

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
CONTENT_TYPE_HEADER="Content-Type: application/json"
REALM_HEADER="Polaris-Realm: ${POLARIS_REALM}"

api() {
  method="$1"
  path="$2"
  data="$3"
  if [ -n "${data}" ]; then
    curl -s -o /dev/null -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
      -H "${AUTH_HEADER}" \
      -H "${CONTENT_TYPE_HEADER}" \
      -H "${REALM_HEADER}" \
      -d "${data}"
  else
    curl -s -o /dev/null -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
      -H "${AUTH_HEADER}" \
      -H "${CONTENT_TYPE_HEADER}" \
      -H "${REALM_HEADER}"
  fi
}

api_response() {
  method="$1"
  path="$2"
  data="$3"
  tmpfile="$(mktemp)"
  if [ -n "${data}" ]; then
    RESPONSE_STATUS=$(curl -s -o "${tmpfile}" -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
      -H "${AUTH_HEADER}" \
      -H "${CONTENT_TYPE_HEADER}" \
      -H "${REALM_HEADER}" \
      -d "${data}")
  else
    RESPONSE_STATUS=$(curl -s -o "${tmpfile}" -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
      -H "${AUTH_HEADER}" \
      -H "${CONTENT_TYPE_HEADER}" \
      -H "${REALM_HEADER}")
  fi
  RESPONSE_BODY="$(cat "${tmpfile}")"
  rm -f "${tmpfile}"
}

check_status() {
  status="$1"
  message="$2"
  allow_existing_grant_status="${3:-false}"
  case "${status}" in
    200|201|204) echo "  -> ${message}" ;;
    409) echo "  -> already exists" ;;
    500)
      if [ "${allow_existing_grant_status}" = "true" ]; then
        echo "  -> already configured"
      else
        echo "ERROR: unexpected status ${status}"
        exit 1
      fi
      ;;
    *) echo "ERROR: unexpected status ${status}"; exit 1 ;;
  esac
}

json_field() {
  printf "%s" "$1" | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p"
}

setup_service_principal() {
  echo ""
  echo "Setting up Polaris service principal ${POLARIS_SERVICE_PRINCIPAL}"

  status=$(api POST "/principals" "{
    \"principal\": {
      \"name\": \"${POLARIS_SERVICE_PRINCIPAL}\",
      \"type\": \"USER\",
      \"properties\": {}
    }
  }")
  check_status "${status}" "service principal created"

  if [ "${POLARIS_SERVICE_PRINCIPAL_ROLE}" = "service_admin" ]; then
    echo "  -> using built-in service_admin principal role"
  else
    status=$(api POST "/principal-roles" "{
      \"principalRole\": {
        \"name\": \"${POLARIS_SERVICE_PRINCIPAL_ROLE}\",
        \"properties\": {}
      }
    }")
    check_status "${status}" "service principal role created"
  fi

  status=$(api PUT "/principals/${POLARIS_SERVICE_PRINCIPAL}/principal-roles" "{
    \"principalRole\": {
      \"name\": \"${POLARIS_SERVICE_PRINCIPAL_ROLE}\"
    }
  }")
  check_status "${status}" "service principal role assigned" true

  api_response POST "/principals/${POLARIS_SERVICE_PRINCIPAL}/reset" "{}"
  check_status "${RESPONSE_STATUS}" "service principal credentials reset"

  client_id="$(json_field "${RESPONSE_BODY}" "clientId")"
  client_secret="$(json_field "${RESPONSE_BODY}" "clientSecret")"
  if [ -z "${client_id}" ] || [ -z "${client_secret}" ]; then
    echo "ERROR: Could not parse service principal credentials. Response: ${RESPONSE_BODY}"
    exit 1
  fi

  mkdir -p "$(dirname "${POLARIS_SERVICE_CREDENTIAL_FILE}")"
  printf "%s:%s" "${client_id}" "${client_secret}" > "${POLARIS_SERVICE_CREDENTIAL_FILE}"
  chmod 600 "${POLARIS_SERVICE_CREDENTIAL_FILE}"
  echo "  -> service principal credentials written"

  SERVICE_TOKEN_RESPONSE=$(curl -sf -X POST "${POLARIS_HOST}/api/catalog/v1/oauth/tokens" \
    -u "${client_id}:${client_secret}" \
    -H "Polaris-Realm: ${POLARIS_REALM}" \
    -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")
  service_token="$(json_field "${SERVICE_TOKEN_RESPONSE}" "access_token")"
  if [ -z "${service_token}" ]; then
    echo "ERROR: Service principal credentials did not produce an OAuth token."
    exit 1
  fi
  echo "  -> service principal credential verified"
}

create_catalog() {
  user="$1"
  catalog="user_${user}"
  location="s3://${WAREHOUSE_BUCKET}/${WAREHOUSE_PREFIX}/${user}/iceberg/"
  role="${catalog}_admin"

  echo ""
  echo "Setting up Polaris catalog ${catalog} at ${location}"

  status=$(api POST "/catalogs" "{
    \"catalog\": {
      \"name\": \"${catalog}\",
      \"type\": \"INTERNAL\",
      \"properties\": {
        \"default-base-location\": \"${location}\"
      },
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [\"${location}\"],
        \"endpoint\": \"${MINIO_ENDPOINT}\",
        \"endpointInternal\": \"${MINIO_ENDPOINT}\",
        \"pathStyleAccess\": true,
        \"stsUnavailable\": true,
        \"region\": \"us-east-1\"
      }
    }
  }")
  check_status "${status}" "catalog created"

  status=$(api POST "/catalogs/${catalog}/catalog-roles" "{
    \"catalogRole\": {
      \"name\": \"${role}\"
    }
  }")
  check_status "${status}" "catalog role created"

  status=$(api PUT "/catalogs/${catalog}/catalog-roles/${role}/grants" "{
    \"grant\": {
      \"type\": \"catalog\",
      \"privilege\": \"CATALOG_MANAGE_CONTENT\"
    }
  }")
  check_status "${status}" "catalog grant applied" true

  if [ "${POLARIS_SERVICE_PRINCIPAL_ROLE}" != "service_admin" ]; then
    status=$(api PUT "/principal-roles/${POLARIS_SERVICE_PRINCIPAL_ROLE}/catalog-roles/${catalog}" "{
      \"catalogRole\": {
        \"name\": \"${role}\"
      }
    }")
    check_status "${status}" "catalog role assigned to ${POLARIS_SERVICE_PRINCIPAL_ROLE}" true
  fi
}

setup_service_principal

for user in ${POLARIS_CATALOGS}; do
  create_catalog "${user}"
done

echo ""
echo "Polaris catalog setup complete."
