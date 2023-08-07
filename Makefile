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

PYTHON ?= python3.6
PREFIX ?= /opt/yandex/ch-tools
OUTPUT_DIR ?= out

# It is used by DEB building tools to install a program to temporary
# directory before packaging
DESTDIR ?=

INSTALL_DIR = $(DESTDIR)$(PREFIX)
BIN_DIR = $(INSTALL_DIR)/bin
SYMLINK_BIN_DIR = $(DESTDIR)/usr/bin

WHL_FILE = ch_tools-*.whl
VENV_DIR = .venv
VERSION_FILE = version.txt
INSTALL_DEPS_MARKER = .install-deps


.PHONY: install
install: install-python-package install-symlinks install-bash-completions configure-logs ;


.PHONY: install-dependencies
install-dependencies: $(INSTALL_DEPS_MARKER) ;


.PHONY: uninstall
uninstall: uninstall-python-package uninstall-symlinks uninstall-bash-completions uninstall-logrotate ;


.PHONY: check-poetry
check-poetry:
	if [ -z $$(command -v poetry 2> /dev/null) ]; then
	  echo "Poetry could not be found. See https://python-poetry.org/docs/"; 
	  exit 1;
	fi


$(VENV_DIR):
	poetry env use $(PYTHON)


.PHONY: venv
venv: check-poetry $(VENV_DIR) ;


$(INSTALL_DEPS_MARKER): check-poetry venv pyproject.toml 
	poetry install --no-root
	touch $(INSTALL_DEPS_MARKER)


.PHONY: lint
lint: install-dependencies
	poetry run black --check --diff ch_tools tests
	poetry run isort --recursive --diff ch_tools tests


.PHONY: unit-tests
unit-tests: install-dependencies
	poetry run $(PYTHON) -m pytest tests/unit


.PHONY: integration-tests
integration-tests: install-dependencies build-python-package	
	cd tests
	poetry run $(PYTHON) -m env_control create
	poetry run behave --show-timings --junit -D skip_setup


.PHONY: install-python-package
install-python-package: build-python-package
	echo 'Installing ch-tools'

	# Prepare new virual environment
	poetry run $(PYTHON) -m venv $(INSTALL_DIR)
	rm -f $(BIN_DIR)/activate*

	# Install python package
	$(BIN_DIR)/pip install --upgrade pip
	$(BIN_DIR)/pip install --no-compile dist/$(WHL_FILE)

	# Clean python's artefacts
	find $(INSTALL_DIR) -name __pycache__ -type d -exec rm -rf {} +

	# Remove DESTDIR prefix from script's shebangs if it's present
	test -n '$(DESTDIR)' \
		&& grep -l -r -F '#!$(INSTALL_DIR)' $(INSTALL_DIR) \
			| xargs sed -i -e 's|$(INSTALL_DIR)|$(PREFIX)|' \
		|| true


.PHONY: build-python-package
build-python-package: prepare-version clean-dist
	echo 'Building python packages...'
	poetry build


.PHONY: clean-dist
clean-dist:
	echo 'Cleaning up residuals from building of Python package'
	sudo rm -rf dist	


.PHONY: uninstall-python-package
uninstall-python-package:
	echo 'Uninstalling ch-tools'
	rm -rf $(INSTALL_DIR)


.PHONY: install-symlinks
install-symlinks:
	echo 'Creating symlinks to $(SYMLINK_BIN_DIR)'

	mkdir -p $(SYMLINK_BIN_DIR)
	$(foreach bin, chadmin ch-monitoring keeper-monitoring ch-s3-credentials, \
		ln -sf $(PREFIX)/bin/$(bin) $(SYMLINK_BIN_DIR);) 


.PHONY: uninstall-symlinks
uninstall-symlinks:
	echo 'Removing symlinks from $(SYMLINK_BIN_DIR)'

	$(foreach bin, chadmin ch-monitoring keeper-monitoring ch-s3-credentials, \
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
prepare-changelog: prepare-version
	echo 'Bumping version into Debian package changelog'
	DEBFULLNAME="Yandex LLC" DEBEMAIL="ch-tools@yandex-team.ru" dch --force-bad-version --distribution stable -v $$(cat $(VERSION_FILE)) Autobuild


.PHONY: prepare-version
prepare-version: $(VERSION_FILE)
	echo "Version: $$(cat $(VERSION_FILE))"


$(VERSION_FILE):
	# Generate version
	echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | xargs -I {} printf '%d' 0x{})" > $
	# Replace version in src/ch_tools/__init__.py
	sed -ie "s/__version__ = \"[0-9\.]\+\"/__version__ = \"$$(cat $@)\"/" src/ch_tools/__init__.py
	# Replace version in pyproject.toml
	poetry version $$(cat $@)


.PHONY: build-deb-package
build-deb-package: prepare-changelog	
	# Build DEB package
	(cd debian && poetry run debuild --check-dirname-level 0 --preserve-env --no-lintian --no-tgz-check -uc -us)
	# Move DEB package to output dir
	DEB_FILE=$$(echo ../ch-tools*.deb)
	mkdir -p $(OUTPUT_DIR) && mv $$DEB_FILE $(OUTPUT_DIR)
	# Remove other build artefacts
	rm $${DEB_FILE%_*.deb}*


.PHONY: clean
clean:
	echo 'Cleaning up'

	rm -rf build
	rm -rf debian/files debian/.debhelper
	rm -rf debian/ch-tools*

 
.PHONY: help
help:
	echo "Base targets:"
	echo "  prepare-changelog          Add an autobuild version entity to changelog"
	echo "  prepare-version            Update version based on latest commit"
	echo "  build-python-package       Build 'ch-tools' Python package"
	echo "  build-deb-package          Build 'ch-tools' debian package"
	echo "  clean                      Clean up after building debian package"
	echo ""
	echo "--------------------------------------------------------------------------------"
	echo ""
	echo "Debian package build targets:"
	echo "  install                    Install 'ch-tools' debian package"
	echo "  uninstall                  Uninstall 'ch-tools' debian package"
	echo ""
	echo "  install-python-package     Install 'ch-tools' python package"
	echo "  uninstall-python-package   Uninstall 'ch-tools' python package"
	echo "  install-symlinks           Install symlinks to /usr/bin/"
	echo "  uninstall-symlinks         Uninstall symlinks from /usr/bin/"
	echo "  install-bash-completions   Install to /etc/bash_completion.d/"
	echo "  uninstall-bash-completions Uninstall from /etc/bash_completion.d/"
	echo "  configure-logs             Install log rotation rules to /etc/logrotate.d/ and create log dirs"
	echo "  uninstall-logrotate        Uninstall log rotation rules from /etc/logrotate.d/"
	echo ""
	echo "--------------------------------------------------------------------------------"
	echo ""
	echo "  help                       Show this help message."
