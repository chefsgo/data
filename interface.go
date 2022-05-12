package data

import (
	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
)

func (this *Module) Register(key string, value Any, override bool) {
	switch val := value.(type) {
	case Driver:
		module.Driver(key, val, override)
	case Config:
		module.Config(key, val, override)
	case Configs:
		module.Configs(key, val, override)
	case Table:
		module.Table(key, val, override)
	case View:
		module.View(key, val, override)
	case Model:
		module.Model(key, val, override)
	}
}

func (this *Module) configure(name string, config Map) {
	cfg := Config{
		Driver: chef.DEFAULT, Serial: "serial",
	}

	//如果已经存在了，用现成的改写
	if vv, ok := module.configs[name]; ok {
		cfg = vv
	}

	if driver, ok := config["driver"].(string); ok {
		cfg.Driver = driver
	}

	if url, ok := config["url"].(string); ok {
		cfg.Url = url
	}
	if serial, ok := config["serial"].(string); ok {
		cfg.Serial = serial
	}
	if setting, ok := config["setting"].(Map); ok {
		cfg.Setting = setting
	}

	//保存配置
	this.configs[name] = cfg
}
func (this *Module) Configure(value Any) {
	if cfg, ok := value.(Config); ok {
		this.configs[chef.DEFAULT] = cfg
		return
	}
	if cfg, ok := value.(map[string]Config); ok {
		this.configs = cfg
		return
	}

	var config Map
	if global, ok := value.(Map); ok {
		if vvv, ok := global["data"].(Map); ok {
			config = vvv
		}
	}
	if config == nil {
		return
	}

	//记录上一层的配置，如果有的话
	rootConfig := Map{}

	for key, val := range config {
		if conf, ok := val.(Map); ok {
			this.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}

	if len(rootConfig) > 0 {
		this.configure(chef.DEFAULT, rootConfig)
	}
}
func (this *Module) Initialize() {
	if this.initialized {
		return
	}

	this.initialized = true
}
func (this *Module) Connect() {
	if this.connected {
		return
	}

	for name, config := range this.configs {
		driver, ok := this.drivers[config.Driver]
		if ok == false {
			panic("Invalid data driver: " + config.Driver)
		}

		// 建立连接
		connect, err := driver.Connect(name, config)
		if err != nil {
			panic("Failed to connect to data: " + err.Error())
		}

		// 打开连接
		err = connect.Open()
		if err != nil {
			panic("Failed to open data connect: " + err.Error())
		}

		//保存连接
		this.instances[name] = Instance{
			name, config, connect,
		}

	}

	this.connected = true
}
func (this *Module) Launch() {
	if this.launched {
		return
	}

	this.launched = true
}
func (this *Module) Terminate() {
	for _, ins := range this.instances {
		ins.connect.Close()
	}

	this.launched = false
	this.connected = false
	this.initialized = false
}
