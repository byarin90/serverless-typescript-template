import axios from 'axios';
import jwt from 'jsonwebtoken';
import { config } from 'dotenv';

config();

const {
  AUTH0_DOMAIN,
  AUTH0_AUDIENCE,
  AUTH0_CLIENT_ID,
  AUTH0_CLIENT_SECRET,
  LOCAL_JWT_SECRET,
} = process.env;

interface Options {
  local?: boolean;
  scope?: string;
}

async function getRemoteToken(scope: string) {
  const url = `${AUTH0_DOMAIN}/oauth/token`;
  const { data } = await axios.post(url, {
    client_id: AUTH0_CLIENT_ID,
    client_secret: AUTH0_CLIENT_SECRET,
    audience: AUTH0_AUDIENCE,
    grant_type: 'client_credentials',
    scope,
  });
  return data.access_token as string;
}

function getLocalToken(scope: string) {
  if (!LOCAL_JWT_SECRET) throw new Error('LOCAL_JWT_SECRET missing');
  const payload = {
    scope,
    iss: AUTH0_DOMAIN,
    aud: AUTH0_AUDIENCE,
  };
  return jwt.sign(payload, LOCAL_JWT_SECRET, { algorithm: 'HS256', expiresIn: '1h' });
}

export default async function generateToken(opts: Options = {}) {
  const scope = opts.scope || '';
  if (opts.local) {
    return getLocalToken(scope);
  }
  return getRemoteToken(scope);
}

if (require.main === module) {
  const local = process.argv.includes('--local');
  const scopeIndex = process.argv.indexOf('--scope');
  const scope = scopeIndex !== -1 ? process.argv[scopeIndex + 1] : '';
  generateToken({ local, scope })
    .then((token) => console.log(token))
    .catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
}
