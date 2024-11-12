import cli from './cli';
import forms from './forms';
import models from './models';
import graph from './graph';
import grpc from './protobuf';
import routes from './routes';
import services from './services';
import translations from './i18n';
import workflows from './workflow';

const ReactoryQueue: Reactory.Server.IReactoryModule = {
  id: 'reactory-queue',
  nameSpace: 'reactory',
  name: 'Queue',
  version: '1.0.0',
  description: 'Reactory Queue abstraction',
  dependencies: ['core.ReactoryServer@1.0.0'],
  priority: 2,
  graphDefinitions: graph,
  workflows,
  forms,
  services,
  translations,
  models,
  clientPlugins: [],
  pdfs: [],
  passportProviders: [],
  grpc,
  routes,
  cli,
};

export default ReactoryQueue;
