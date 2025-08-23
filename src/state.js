import { SSMClient, GetParameterCommand, PutParameterCommand } from '@aws-sdk/client-ssm';

const ssm = new SSMClient({});
const prefix = process.env.SSM_PATH;

export async function getCursor(key) {
  try {
    const out = await ssm.send(new GetParameterCommand({ Name: `${prefix}/${key}` }));
    return out.Parameter?.Value || null;
  } catch (_) { return null; }
}

export async function setCursor(key, value) {
  await ssm.send(new PutParameterCommand({ Name: `${prefix}/${key}`, Value: value, Type: 'String', Overwrite: true }));
}
