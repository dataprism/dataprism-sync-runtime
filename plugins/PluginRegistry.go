package plugins

import (
	"errors"
	"github.com/sirupsen/logrus"
)

type PluginRegistry struct {
	plugins map[string]*DataprismSyncPlugin
	inputs map[string]*InputType
	outputs map[string]*OutputType
}

func NewSyncPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		make(map[string]*DataprismSyncPlugin, 1),
		make(map[string]*InputType, 1),
		make(map[string]*OutputType, 1),
	}
}

func (r *PluginRegistry) Add(p *DataprismSyncPlugin) error {
	if _, ok := r.plugins[p.Id]; ok {
		return errors.New("a plugin with id " + p.Id + " has already been registered")
	}

	logrus.Infof("Added the %s plugin", p.Id)
	r.plugins[p.Id] = p

	for _, v := range p.InputTypes { r.inputs[v.Id] = v }
	for _, v := range p.OutputTypes { r.outputs[v.Id] = v }

	return nil
}

func (r *PluginRegistry) GetInputType(id string) (*InputType, bool) {
	res, ok := r.inputs[id]
	return res, ok
}

func (r *PluginRegistry) GetInputTypes() map[string]*InputType {
	return r.inputs
}

func (r *PluginRegistry) GetOutputTypes() map[string]*OutputType {
	return r.outputs
}

func (r *PluginRegistry) GetOutputType(id string) (*OutputType, bool) {
	res, ok := r.outputs[id]
	return res, ok
}

func (r *PluginRegistry) Plugins() []string {
	keys := make([]string, 0, len(r.plugins))
	for k := range r.plugins {
		keys = append(keys, k)
	}

	return keys
}