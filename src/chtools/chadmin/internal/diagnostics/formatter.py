from humanfriendly import format_size


def format_resource_preset(dbaas_config):
    name = dbaas_config.flavor

    if dbaas_config.cpu_fraction < 100:
        cpu = f"{int(dbaas_config.cpu_limit)} * {dbaas_config.cpu_fraction}% CPU cores"
    else:
        cpu = f"{int(dbaas_config.cpu_guarantee)} CPU cores"

    memory = f"{format_size(dbaas_config.memory_guarantee, binary=True)} memory"

    return f"{name} ({cpu} / {memory})"


def format_storage(dbaas_config, ch_config):
    disk_type = dbaas_config.disk_type
    disk_size = format_size(dbaas_config.disk_size, binary=True)

    storage = f"{disk_size} {disk_type}"
    if ch_config.has_disk("object_storage"):
        storage += " + S3"

    return storage
