#!/usr/bin/env bash

set -e

# Function to check the availability of a command
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo "Error: '$1' command not found. Please install it before running this script." >&2
        exit 1
    fi
}

# Function to securely import the private key
import_private_key() {
    local key_path="$1"
    local key_id

    gpg --import --with-colons "$key_path"
    key_id=$(gpg --list-keys --with-colon | awk -F: '/^fpr/ {print $10;exit}')
    
    if [[ -z $key_id ]]; then
        echo "Error: Unable to import signing key from path: $key_path" >&2
        exit 1
    fi
    
    echo "$key_id"
}

# Checking the availability of the necessary commands
check_command "gpg"
check_command "debuild"

# Sanitize package signing options
COUNT=0
for sign_param in DEB_SIGN_KEY DEB_SIGN_KEY_ID DEB_SIGN_KEY_PATH; do
    if [[ -n "${!sign_param}" ]]; then ((COUNT+=1)); fi
done

if (( COUNT > 1 )); then
    echo "Error: At most one of DEB_SIGN_KEY or DEB_SIGN_KEY_ID or DEB_SIGN_KEY_PATH vars must be defined." >&2
    exit 1
fi

# Import GPG signing private key if it is provided
if [[ -n "${DEB_SIGN_KEY_ID}" ]]; then
    # Check if gpg knows about this key id
    if ! gpg --list-keys "${DEB_SIGN_KEY_ID}" &> /dev/null; then
        echo "Error: No public key ${DEB_SIGN_KEY_ID}" >&2
        exit 1
    else
        SIGN_ARGS="-k${DEB_SIGN_KEY_ID}"
    fi
elif [[ -n "${DEB_SIGN_KEY}" ]]; then
    KEY_ID=$(echo "${DEB_SIGN_KEY}" | gpg --import | awk -F: '/^fpr/ {print $10;exit}')
    SIGN_ARGS="-k${KEY_ID}"
elif [[ -n "${DEB_SIGN_KEY_PATH}" ]]; then
    KEY_ID=$(import_private_key "${DEB_SIGN_KEY_PATH}")
    SIGN_ARGS="-k${KEY_ID}"
else
    # Do not sign debian package
    SIGN_ARGS="-us -uc"
fi

# Build package
(cd debian && debuild --preserve-env --check-dirname-level 0 "${SIGN_ARGS}")

# Move debian package and signed metadata files to the output dir
DEB_FILES=$(echo ../${PROJECT_NAME}*.{deb,dsc,changes,buildinfo,tar.*})
mkdir -p "${BUILD_DEB_OUTPUT_DIR}" && mv $DEB_FILES "${BUILD_DEB_OUTPUT_DIR}"
