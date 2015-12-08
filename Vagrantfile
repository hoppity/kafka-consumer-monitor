# -*- mode: ruby -*-
# vi: set ft=ruby :

box    = "williamyeh/ubuntu-trusty64-docker"
Vagrant.configure(2) do |config|
  config.vm.box = "williamyeh/ubuntu-trusty64-docker"
  config.vm.box_check_update = false

  config.vm.network "private_network", ip: "192.168.33.30"
  config.vm.network "forwarded_port", guest: 8000, host: 8000

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  config.vm.synced_folder "./", "/vagrant_data"

  config.vm.provision "docker" do |docker|
      #docker.build_image "/vagrant", args:"-t wh/kafka-monitor"
      docker.images = ["node"]
      docker.run "node",
        args:   "-d -p 8000:8000"\
                " -v '/vagrant_data:/usr/src/app'"
                " -e ZOOKEEPER_CONNECT='192.168.33.10:2181'"
  end


  config.vm.provider "virtualbox" do |vb|
      vb.gui = false
      vb.memory = "1024"
  end
end
