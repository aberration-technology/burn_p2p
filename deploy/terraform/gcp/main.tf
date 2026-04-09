data "google_compute_image" "ubuntu" {
  family  = "ubuntu-2404-lts-amd64"
  project = "ubuntu-os-cloud"
}

locals {
  labels = {
    project = "burn-p2p"
    stack   = var.name_prefix
  }
}

resource "google_compute_firewall" "burn_p2p" {
  name    = "${var.name_prefix}-allow"
  network = var.network

  allow {
    protocol = "tcp"
    ports    = var.allowed_tcp_ports
  }

  allow {
    protocol = "udp"
    ports    = var.allowed_udp_ports
  }

  source_ranges = var.allowed_source_ranges
  target_tags   = [var.name_prefix]
}

resource "google_compute_instance" "bootstrap" {
  count        = var.bootstrap_count
  name         = format("%s-bootstrap-%02d", var.name_prefix, count.index + 1)
  machine_type = var.bootstrap_machine_type
  zone         = var.zone
  tags         = [var.name_prefix]
  labels       = merge(local.labels, { role = "bootstrap" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null
    access_config {}
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name = "${var.name_prefix}-bootstrap"
    image        = var.bootstrap_image
    command      = var.bootstrap_container_command
    config_json  = var.bootstrap_config_json
    gpu_enabled  = false
  })
}

resource "google_compute_instance" "validator" {
  count        = var.validator_count
  name         = format("%s-validator-%02d", var.name_prefix, count.index + 1)
  machine_type = var.validator_machine_type
  zone         = var.zone
  tags         = [var.name_prefix]
  labels       = merge(local.labels, { role = "validator" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null
    access_config {}
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name = "${var.name_prefix}-validator"
    image        = var.validator_image
    command      = var.validator_container_command
    config_json  = var.validator_config_json
    gpu_enabled  = false
  })
}

resource "google_compute_instance" "reducer" {
  count        = var.reducer_count
  name         = format("%s-reducer-%02d", var.name_prefix, count.index + 1)
  machine_type = var.reducer_machine_type
  zone         = var.zone
  tags         = [var.name_prefix]
  labels       = merge(local.labels, { role = "reducer" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null
    access_config {}
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name = "${var.name_prefix}-reducer"
    image        = var.reducer_image
    command      = var.reducer_container_command
    config_json  = var.reducer_config_json
    gpu_enabled  = false
  })
}

resource "google_compute_instance" "trainer" {
  count        = var.trainer_count
  name         = format("%s-trainer-%02d", var.name_prefix, count.index + 1)
  machine_type = var.trainer_machine_type
  zone         = var.zone
  tags         = [var.name_prefix]
  labels       = merge(local.labels, { role = "trainer" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null
    access_config {}
  }

  guest_accelerator {
    type  = var.trainer_accelerator_type
    count = var.trainer_accelerator_count
  }

  scheduling {
    on_host_maintenance = "TERMINATE"
    automatic_restart   = true
  }

  metadata = {
    install-nvidia-driver = "true"
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name = "${var.name_prefix}-trainer"
    image        = var.trainer_image
    command      = var.trainer_container_command
    config_json  = var.trainer_config_json
    gpu_enabled  = true
  })
}
