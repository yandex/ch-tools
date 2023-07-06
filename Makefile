#!/usr/bin/make -f

PREFIX=/opt/yandex/mdb-ch-tools
INSTALL_DIR=$(DESTDIR)$(PREFIX)

.PHONY: install
install: install-python-package install-symlinks install-bash-completions install-logrotate ;

.PHONY: uninstall
uninstall: uninstall-python-package uninstall-symlinks uninstall-bash-completions uninstall-logrotate ;

.PHONY: uninstall-python-package
uninstall-python-package:
	@echo 'Uninstalling mdb-ch-tools'
	rm -rf $(INSTALL_DIR)

.PHONY: install-symlinks
install-symlinks:
	@echo 'Creating symlinks to /usr/bin/'

	mkdir -p $(DESTDIR)/usr/bin/
	$(foreach bin, chadmin ch-monitoring keeper-monitoring ch-s3-credentials, \
		ln -sf $(PREFIX)/bin/$(bin) $(DESTDIR)/usr/bin/ ; \
	)

.PHONY: uninstall-symlinks
uninstall-symlinks:
	@echo 'Removing symlinks from /usr/bin/'

	$(foreach bin, chadmin ch-monitoring keeper-monitoring ch-s3-credentials, \
		rm -f $(DESTDIR)/usr/bin/$(bin) ; \
	)

.PHONY: install-bash-completions
install-bash-completions:
	@echo 'Creating bash completions'

	mkdir -p $(DESTDIR)/etc/bash_completion.d/
	$(foreach bin, chadmin ch-monitoring keeper-monitoring, \
		cp resources/completion/$(bin)-completion.bash $(DESTDIR)/etc/bash_completion.d/$(bin) ; \
	)

.PHONY: uninstall-bash-completions
uninstall-bash-completions:
	@echo 'Removing bash completions'

	$(foreach bin, chadmin ch-monitoring keeper-monitoring, \
		rm -f $(DESTDIR)/etc/bash_completion.d/$(bin) ; \
	)

.PHONY: install-logrotate
install-logrotate:
	@echo 'Creating log rotation rules'

	mkdir -p $(DESTDIR)/etc/logrotate.d/
	$(foreach bin, chadmin clickhouse-monitoring keeper-monitoring, \
		mkdir -p $(DESTDIR)/var/log/$(folder) ; \
		chmod 775 $(DESTDIR)/var/log/$(folder) ; \
		cp resources/logrotate/$(bin).logrotate $(DESTDIR)/etc/logrotate.d/$(bin) ; \
	)

.PHONY: uninstall-logrotate
uninstall-logrotate:
	@echo 'Removing log rotation rules'

	$(foreach bin, chadmin clickhouse-monitoring keeper-monitoring, \
		rm -f $(DESTDIR)/etc/logrotate.d/$(bin) ; \
	)


.PHONY: prepare-changelog
prepare-changelog: prepare-version
	dch --force-bad-version --distribution stable -v `cat version.txt` Autobuild

.PHONY: prepare-version
prepare-version: version.txt
	@echo "Version: `cat version.txt`"

version.txt:
	@echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | perl -ne 'print hex $$_')" > $@

build-deb-package: prepare-changelog
	cd debian && debuild --check-dirname-level 0 --preserve-env --no-lintian --no-tgz-check -uc -us

export CLICKHOUSE_VERSION?=latest

test-integration-prepare:
	cp dist/*.whl tests/
	cd tests && CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION} python3 -m env_control create

test-integration:
	cd tests && behave --show-timings -D skip_setup --junit

.PHONY: help
help:
	@echo "Base targets:"
	@echo "  prepare-changelog          Add an autobuild version entity to changelog"
	@echo "  prepare-version            Update version based on latest commit"
	@echo "  build-deb-package          Build 'mdb-ch-tools' debian package"
	@echo "  clean                      Clean up after building debian package"
	@echo ""
	@echo "--------------------------------------------------------------------------------"
	@echo ""
	@echo "Debian package build targets:"
	@echo "  install                    Install 'mdb-ch-tools' debian package"
	@echo "  uninstall                  Uninstall 'mdb-ch-tools' debian package"
	@echo ""
	@echo "  install-python-package     Install 'ch-tools' python package"
	@echo "  uninstall-python-package   Uninstall 'ch-tools' python package"
	@echo "  install-symlinks           Install symlinks to /usr/bin/"
	@echo "  uninstall-symlinks         Uninstall symlinks from /usr/bin/"
	@echo "  install-bash-completions   Install to /etc/bash_completion.d/"
	@echo "  uninstall-bash-completions Uninstall from /etc/bash_completion.d/"
	@echo "  install-logrotate          Install log rotation rules to /etc/logrotate.d/"
	@echo "  uninstall-logrotate        Uninstall log rotation rules from /etc/logrotate.d/"
	@echo ""
	@echo "--------------------------------------------------------------------------------"
	@echo ""
	@echo "  help                       Show this help message."
