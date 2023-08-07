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

PYTHON ?= python3

# The latest version supporting Python 3.6
POETRY_VERSION ?= 1.1.15
POETRY_HOME ?= /opt/poetry
POETRY := $(POETRY_HOME)/bin/poetry

PREFIX ?= /opt/yandex/ch-tools
BUILD_PYTHON_OUTPUT_DIR ?= dist
BUILD_DEB_OUTPUT_DIR ?= out

# It is used by DEB building tools to install a program to temporary
# directory before packaging
DESTDIR ?=

INSTALL_DIR = $(DESTDIR)$(PREFIX)
BIN_DIR = $(INSTALL_DIR)/bin
SYMLINK_BIN_DIR = $(DESTDIR)/usr/bin

WHL_FILE = ch_tools-*.whl
VENV_DIR = .venv
VERSION_FILE = version.txt
INSTALL_DEPS_STAMP = .install-deps

# Pass arguments for testing tools
BEHAVE_ARGS ?= -D skip_setup
PYTEST_ARGS ?=


.PHONY: install
install: install-python-package install-symlinks install-bash-completions configure-logs ;


.PHONY: uninstall
uninstall: uninstall-python-package uninstall-symlinks uninstall-bash-completions uninstall-logrotate ;


.PHONY: install-deps
install-deps: $(INSTALL_DEPS_STAMP) ;


# Update dependencies in poetry.lock to their latest versions according to your pyproject.toml
.PHONY: update-deps
update-deps:
	$(POETRY) update


$(INSTALL_DEPS_STAMP): ensure-poetry venv pyproject.toml 
	$(POETRY) install --no-root
	touch $(INSTALL_DEPS_STAMP)

.PHONY: install-poetry
install-poetry:
	if [ ! -e $(POETRY) ]; then
		echo "Installing poetry $(POETRY_VERSION)..."
		curl -sSL https://install.python-poetry.org | POETRY_HOME=$(POETRY_HOME) $(PYTHON) - --version $(POETRY_VERSION)

		# Fix cannot "import name 'appengine' from 'urllib3.contrib'..." error 
		# while 'poetry publish' for version poetry 1.1.15
		# https://urllib3.readthedocs.io/en/stable/v2-migration-guide.html#importerror-cannot-import-name-gaecontrib-from-requests-toolbelt-compat
		$(POETRY_HOME)/venv/bin/python -m pip install urllib3==1.26.15
	else
		echo "Found installed poetry $$($(POETRY) --version)"				
	fi

.PHONY: uninstall-poetry
uninstall-poetry:
	echo "Uninstalling poetry..."
	curl -sSL https://install.python-poetry.org | POETRY_HOME=$(POETRY_HOME) $(PYTHON) - --uninstall


.PHONY: ensure-poetry
ensure-poetry:
	if [ ! -e $(POETRY) ]; then
	  echo "Poetry could not be found. Please install it manually 'make install-poetry'"; 
	  exit 1;
	fi


.PHONY: venv
venv: ensure-poetry $(VENV_DIR) ;

$(VENV_DIR):
	$(POETRY) config virtualenvs.in-project true
	$(POETRY) env use $(PYTHON)


.PHONY: lint
lint: install-deps
	$(POETRY) run black --check --diff ch_tools tests
	$(POETRY) run isort --diff ch_tools tests


.PHONY: unit-tests
unit-tests: install-deps
	$(POETRY) run $(PYTHON) -m pytest $(PYTEST_ARGS) tests/unit


.PHONY: integration-tests
integration-tests: install-deps build-python-packages	
	cd tests
	$(POETRY) run $(PYTHON) -m env_control create
	$(POETRY) run behave --show-timings --junit $(BEHAVE_ARGS)


.PHONY: publish
publish:
	$(POETRY) publish


.PHONY: install-python-package
install-python-package: build-python-packages
	echo 'Installing ch-tools'

	# Prepare new virual environment
	$(POETRY) run $(PYTHON) -m venv $(INSTALL_DIR)
	rm -f $(BIN_DIR)/activate*

	# Install python package
	$(BIN_DIR)/pip install --upgrade pip
	$(BIN_DIR)/pip install --no-compile $(BUILD_PYTHON_OUTPUT_DIR)/$(WHL_FILE)

	# Clean python's artefacts
	find $(INSTALL_DIR) -name __pycache__ -type d -exec rm -rf {} +

	# Remove DESTDIR prefix from script's shebangs if it's present
	test -n '$(DESTDIR)' \
		&& grep -l -r -F '#!$(INSTALL_DIR)' $(INSTALL_DIR) \
			| xargs sed -i -e 's|$(INSTALL_DIR)|$(PREFIX)|' \
		|| true


.PHONY: build-python-packages
build-python-packages: install-deps prepare-version clean-dist
	echo 'Building python packages...'
	$(POETRY) build


.PHONY: clean-dist
clean-dist:
	echo 'Cleaning up residuals from building of Python package'
	sudo rm -rf $(BUILD_PYTHON_OUTPUT_DIR)


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
	echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | xargs -I {} printf '%d' 0x{})" > $@
	# Replace version in ch_tools/__init__.py
	sed "s/__version__ = \"[0-9\.]\+\"/__version__ = \"$$(cat $@)\"/" ch_tools/__init__.py
	# Replace version in pyproject.toml
	$(POETRY) version $$(cat $@)


.PHONY: prepare-build-deb
prepare-build-deb:
	apt install python3-venv debhelper devscripts


.PHONY: build-deb-package
build-deb-package: prepare-changelog install-deps
	# Build DEB package
	(cd debian && $(POETRY) run debuild --check-dirname-level 0 --preserve-env --no-lintian --no-tgz-check -uc -us)
	# Move DEB package to output dir
	DEB_FILE=$$(echo ../ch-tools*.deb)
	mkdir -p $(BUILD_DEB_OUTPUT_DIR) && mv $$DEB_FILE $(BUILD_DEB_OUTPUT_DIR)


.PHONY: clean_debuild
clean_debuild:
	rm -rf debian/{files,.debhelper,ch-tools*}
	rm -f ../ch-tools_*{build,buildinfo,changes,deb,dsc,gz,xz}


.PHONY: clean
clean: clean_debuild
	echo 'Cleaning up'

	rm -rf $(BUILD_DEB_OUTPUT_DIR)
	rm -rf $(BUILD_PYTHON_OUTPUT_DIR)
	rm -rf $(VENV_DIR)

 
.PHONY: help
help:
	echo "Base targets:"
	echo "  install-poetry             Install Poetry"
	echo "  uninstall-poetry           Uninstall Poetry"
	echo "  install-deps               Install Python dependencies to local environment $(VENV_DIR)"
	echo "  update-deps                Update dependencies in poetry.lock to their latest versions"	
	echo "  publish                    Publish python package to PYPI"	
	echo "  lint           	   		   Run linters"
	echo "  unit-tests           	   Run unit tests"
	echo "  integration-tests          Run integration tests"
	echo "  prepare-changelog          Add an autobuild version entity to changelog"
	echo "  prepare-version            Update version based on latest commit"
	echo "  build-python-packages      Build 'ch-tools' Python package"
	echo "  build-deb-package          Build 'ch-tools' debian package"
	echo "  clean                      Clean-up all produced/generated files inside tree"
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
	echo "  prepare-build-deb          Install prerequisites for DEB packaging tool"
	echo ""
	echo "--------------------------------------------------------------------------------"
	echo ""
	echo "  help                       Show this help message."
