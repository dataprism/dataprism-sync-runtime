package main

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
	"errors"
	"github.com/lytics/logrus"
)

type PluginRegistry struct {
	plugins map[string]plugins.DataprismSyncPlugin
	inputs map[string]*plugins.InputType
	outputs map[string]*plugins.OutputType
}

func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{}
}

func (r *PluginRegistry) Add(p plugins.DataprismSyncPlugin) error {
	if _, ok := r.plugins[p.Id()]; ok {
		return errors.New("a plugin with id " + p.Id() + " has already been registered")
	}

	logrus.Infof("Added the %s plugin", p.Id())
	r.plugins[p.Id()] = p

	for _, v := range p.InputTypes() { r.inputs[v.Id] = v }
	for _, v := range p.OutputTypes() { r.outputs[v.Id] = v }

	return nil
}

func (r *PluginRegistry) GetInputType(id string) (*plugins.InputType, bool) {
	res, ok := r.inputs[id]
	return res, ok
}

func (r *PluginRegistry) GetInputTypes() map[string]*plugins.InputType {
	return r.inputs
}

func (r *PluginRegistry) GetOutputTypes() map[string]*plugins.OutputType {
	return r.outputs
}

func (r *PluginRegistry) GetOutputType(id string) (*plugins.InputType, bool) {
	res, ok := r.inputs[id]
	return res, ok
}

func (r *PluginRegistry) Plugins(p plugins.DataprismSyncPlugin) map[string]plugins.DataprismSyncPlugin {
	return r.plugins
}