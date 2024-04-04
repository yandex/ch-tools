import logging


class Chadmin:
    def __init__(self, container):
        self._container = container

    def exec_cmd(self, cmd):
        ch_admin_cmd = f"chadmin {cmd}"
        logging.debug("chadmin command:", ch_admin_cmd)
        result = self._container.exec_run(["bash", "-c", ch_admin_cmd], user="root")
        return result

    def create_zk_node(self, zk_node, no_ch_config=True, recursive=True):
        cmd = "zookeeper {use_config} create {make_parents} {node}".format(
            use_config="--no-ch-config" if no_ch_config else "",
            make_parents="--make-parents" if recursive else "",
            node=zk_node,
        )
        return self.exec_cmd(cmd)

    def zk_delete(self, zk_nodes, no_ch_config=False):
        cmd = "zookeeper {use_config} delete {nodes}".format(
            use_config="--no-ch-config" if no_ch_config else "",
            nodes=zk_nodes,
        )
        return self.exec_cmd(cmd)

    def zk_list(self, zk_node, no_ch_config=False):
        cmd = "zookeeper {use_config} list {node}".format(
            use_config="--no-ch-config" if no_ch_config else "",
            node=zk_node,
        )
        return self.exec_cmd(cmd)

    def zk_cleanup(self, fqdn, zk_root=None, no_ch_config=False):
        cmd = "zookeeper {use_config} {root} cleanup-removed-hosts-metadata {hosts}".format(
            use_config="--no-ch-config" if no_ch_config else "",
            root=f"--chroot {zk_root}" if zk_root else "",
            hosts=fqdn,
        )
        return self.exec_cmd(cmd)

    def zk_cleanup_table(self, fqdn, zk_table_path_):
        cmd = "zookeeper remove-hosts-from-table {zk_table_path} {hosts}".format(
            zk_table_path=zk_table_path_,
            hosts=fqdn,
        )
        return self.exec_cmd(cmd)
