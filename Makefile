#!/usr/bin/make -f

ifndef VERBOSE
.SILENT:
endif

SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

# For install target stability
export LC_ALL = en_US.UTF-8
export LANG   = en_US.UTF-8

export PROJECT_NAME ?= clickhouse-tools
export PROJECT_NAME_UNDERSCORE ?= $(subst -,_,$(PROJECT_NAME))

export PYTHON_VERSION ?= $(shell cat .python-version)

PREFIX ?= /opt/yandex/$(PROJECT_NAME)
export BUILD_PYTHON_OUTPUT_DIR ?= dist
export BUILD_DEB_OUTPUT_DIR ?= out
SRC_DIR ?= ch_tools
TESTS_DIR ?= tests

# It is used by DEB building tools to install a program to temporary
# directory before packaging
DESTDIR ?=

INSTALL_DIR = $(DESTDIR)$(PREFIX)
BIN_DIR = $(INSTALL_DIR)/bin
SYMLINK_BIN_DIR = $(DESTDIR)/usr/bin

WHL_FILE := $(PROJECT_NAME_UNDERSCORE)-*.whl
VENV_DIR := .venv
VERSION_FILE := ch_tools/version.txt

# Pass arguments for testing tools
BEHAVE_ARGS ?=
PYTEST_ARGS ?=

# Different ways of passing signing key for building debian package
export DEB_SIGN_KEY_ID ?=
export DEB_SIGN_KEY ?=
export DEB_SIGN_KEY_PATH ?=

# Platform of image for building debian package according to
# https://docs.docker.com/build/building/multi-platform/#building-multi-platform-images
# E.g. linux/amd64, linux/arm64, etc.
# If platform is not provided Docker uses platform of the host performing the build
export DEB_TARGET_PLATFORM ?=
# Name of image (with tag) for building deb package.
# E.g. ubuntu:22.04, ubuntu:jammy, ubuntu:bionic, etc.
# If it is not provided, default value in Dockerfile is used
export DEB_BUILD_DISTRIBUTION ?=


# Should be default target, because "make" is just run
# by debhelper(dh_auto_build stage) for building the program during
# creation of a DEB package
.PHONY: build
build: setup build-python-packages


.PHONY: setup
setup: check-environment $(VERSION_FILE)


.PHONY: all
all: lint test-unit build test-integration


.PHONY: lint
lint: isort black codespell ruff pylint mypy


.PHONY: isort
isort: setup
	uv run --python $(PYTHON_VERSION) isort --check --diff $(SRC_DIR) $(TESTS_DIR)


.PHONY: black
black: setup
	uv run --python $(PYTHON_VERSION) black --check --diff $(SRC_DIR) $(TESTS_DIR)


.PHONY: codespell
codespell: setup
	uv run --python $(PYTHON_VERSION) codespell $(SRC_DIR) $(TESTS_DIR)

.PHONY: fix-codespell-errors
fix-codespell-errors: setup
	uv run --python $(PYTHON_VERSION) codespell -w $(SRC_DIR) $(TESTS_DIR)


.PHONY: ruff
ruff: setup
	uv run --python $(PYTHON_VERSION) ruff check $(SRC_DIR) $(TESTS_DIR)


.PHONY: pylint
pylint: setup
	uv run --python $(PYTHON_VERSION) pylint $(SRC_DIR)


.PHONY: mypy
mypy: setup
	uv run --python $(PYTHON_VERSION) mypy $(SRC_DIR) $(TESTS_DIR)


.PHONY: format
format: setup
	uv run --python $(PYTHON_VERSION) isort $(SRC_DIR) $(TESTS_DIR)
	uv run --python $(PYTHON_VERSION) black $(SRC_DIR) $(TESTS_DIR)


.PHONY: test-unit
test-unit: setup
	uv run --python $(PYTHON_VERSION) py.test $(PYTEST_ARGS) $(TESTS_DIR)/unit


.PHONY: test-integration
test-integration: build-python-packages
	cd $(TESTS_DIR)
	export PYTHONPATH=$(CURDIR):$$PATH
	uv run --python $(PYTHON_VERSION) behave --show-timings --stop --junit $(BEHAVE_ARGS)


.PHONY: publish
publish:
	uv publish --python $(PYTHON_VERSION)


.PHONY: install
install: install-python-package install-symlinks install-bash-completions configure-logs


.PHONY: uninstall
uninstall: uninstall-python-package uninstall-symlinks uninstall-bash-completions uninstall-logrotate


.PHONY: install-python-package
install-python-package: build-python-packages
	echo 'Installing $(PROJECT_NAME)'

	# Prepare new virtual environment
	python3 -m venv $(INSTALL_DIR)
	rm -f $(BIN_DIR)/activate*

	# Install python package
	$(BIN_DIR)/pip install --no-compile $(BUILD_PYTHON_OUTPUT_DIR)/$(WHL_FILE)

	# Clean python's artefacts
	find $(INSTALL_DIR) -name __pycache__ -type d -exec rm -rf {} +

	# Remove DESTDIR prefix from script's shebangs if it's present
	test -n '$(DESTDIR)' \
		&& grep -l -r -F '#!$(INSTALL_DIR)' $(INSTALL_DIR) \
			| xargs sed -i -e 's|$(INSTALL_DIR)|$(PREFIX)|' \
		|| true


.PHONY: build-python-packages
build-python-packages: setup
	echo 'Building python packages...'
	uv build --python $(PYTHON_VERSION)


.PHONY: clean-dist
clean-dist:
	echo 'Cleaning up residuals from building of Python package'
	rm -rf $(BUILD_PYTHON_OUTPUT_DIR)


.PHONY: uninstall-python-package
uninstall-python-package:
	echo 'Uninstalling $(PROJECT_NAME)'
	rm -rf $(INSTALL_DIR)


.PHONY: install-symlinks
install-symlinks:
	echo 'Creating symlinks to $(SYMLINK_BIN_DIR)'

	mkdir -p $(SYMLINK_BIN_DIR)
	$(foreach bin, chadmin ch-monitoring keeper-monitoring, \
		ln -sf $(PREFIX)/bin/$(bin) $(SYMLINK_BIN_DIR);)


.PHONY: uninstall-symlinks
uninstall-symlinks:
	echo 'Removing symlinks from $(SYMLINK_BIN_DIR)'

	$(foreach bin, chadmin ch-monitoring keeper-monitoring, \
	    rm -f $(SYMLINK_BIN_DIR)/$(bin);)


.PHONY: install-bash-completions
install-bash-completions:
	echo 'Creating bash completions'

	mkdir -p $(DESTDIR)/etc/bash_completion.d/
	$(foreach bin, chadmin ch-monitoring keeper-monitoring, \
	    cp resources/completion/$(bin)-completion.bash $(DESTDIR)/etc/bash_completion.d/$(bin);)


.PHONY: uninstall-bash-completions
uninstall-bash-completions:
	echo 'Removing bash completions'

	$(foreach bin, chadmin ch-monitoring keeper-monitoring, \
	    rm -f $(DESTDIR)/etc/bash_completion.d/$(bin);)


.PHONY: configure-logs
configure-logs:
	echo 'Configuring logging'

	mkdir -p $(DESTDIR)/etc/logrotate.d/
	$(foreach bin, chadmin clickhouse-monitoring keeper-monitoring, \
		mkdir -p $(DESTDIR)/var/log/$(bin) ; \
		chmod 775 $(DESTDIR)/var/log/$(bin) ; \
		cp resources/logrotate/$(bin).logrotate $(DESTDIR)/etc/logrotate.d/$(bin);)


.PHONY: uninstall-logrotate
uninstall-logrotate:
	echo 'Removing log rotation rules'

	$(foreach bin, chadmin clickhouse-monitoring keeper-monitoring, \
		rm -f $(DESTDIR)/etc/logrotate.d/$(bin);)


.PHONY: prepare-changelog
prepare-changelog:
	echo 'Bumping version into Debian package changelog'
	DEBFULLNAME="Yandex LLC" DEBEMAIL="ch-tools@yandex-team.ru" dch --force-bad-version --distribution stable -v $$(cat $(VERSION_FILE)) Autobuild


.PHONY: check-environment
check-environment:
	@if ! command -v "uv" &>/dev/null; then \
		echo 'Python project manager tool "uv" not found. Please follow installation instructions at https://docs.astral.sh/uv/getting-started/installation.' >&2; exit 1; \
	fi
	@if [ -z "${PYTHON_VERSION}" ]; then \
		echo 'Failed to determine version of Python interpreter to use.' >&2; exit 1; \
	fi


$(VERSION_FILE):
	echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | xargs -I {} printf '%d' 0x{})" > $@


.PHONY: prepare-build-deb
prepare-build-deb:
	apt install python3-venv debhelper devscripts


.PHONY: build-deb-package
build-deb-package: setup
	./build_deb_in_docker.sh


.PHONY: build-deb-package-local
build-deb-package-local: prepare-changelog
	./build_deb.sh


.PHONY: clean_debuild
clean_debuild:
	rm -rf debian/{files,.debhelper,$(PROJECT_NAME)*,*stamp}
	rm -f ../$(PROJECT_NAME)_*{build,buildinfo,changes,deb,dsc,gz,xz}


.PHONY: clean
clean: clean_debuild
	echo 'Cleaning up'

	rm -rf $(BUILD_DEB_OUTPUT_DIR)
	rm -rf $(BUILD_PYTHON_OUTPUT_DIR)
	rm -rf $(VENV_DIR)
	rm -rf .mypy_cache .ruff_cache tests/{.session_conf.sav,__pycache__,staging,reports}


.PHONY: help
help:
	echo "Targets:"
	echo "  build (default)            Build Python packages (sdist and wheel)."
	echo "  all                        Alias for \"lint test-unit build test-integration\"."
	echo "  lint                       Run linters. Alias for \"isort black codespell ruff pylint mypy\"."
	echo "  test-unit                  Run unit tests."
	echo "  test-integration           Run integration tests."
	echo "  isort                      Perform isort checks."
	echo "  black                      Perform black checks."
	echo "  codespell                  Perform codespell checks."
	echo "  ruff                       Perform ruff checks."
	echo "  pylint                     Perform pylint checks."
	echo "  mypy                       Perform mypy checks."
	echo "  build-deb-package          Build Debian package."
	echo "  publish                    Publish Python package to PyPI"
	echo "  format                     Re-format source code to conform style settings enforced by"
	echo "                             isort and black tools."
	echo "  clean                      Clean up build and test artifacts."
	echo "  help                       Show this help message."
	echo
	echo "Environment Variables:"
	echo "  PYTHON_VERSION             Python version to use (default: \"$(PYTHON_VERSION)\")."
	echo "  PYTEST_ARGS                Arguments to pass to pytest (unit tests)."
	echo "  BEHAVE_ARGS                Arguments to pass to behave (integration tests)."
	echo "  CLICKHOUSE_VERSION         ClickHouse version to use in integration tests (default: \"$(CLICKHOUSE_VERSION)\")."
