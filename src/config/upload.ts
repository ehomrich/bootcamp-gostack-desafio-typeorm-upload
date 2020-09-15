import path from 'path';
import crypto from 'crypto';
import multer from 'multer';

const tmpFolder = path.resolve(__dirname, '..', '..', 'tmp');

export default {
  directory: tmpFolder,
  storage: multer.diskStorage({
    destination: tmpFolder,
    filename(request, file, callback) {
      const hash = crypto.randomBytes(10).toString('hex');
      const slug = file.originalname
        .split(' ')
        .map(s => s.toLowerCase())
        .join('-');
      const filename = `${hash}-${slug}`;

      return callback(null, filename);
    },
  }),
};
