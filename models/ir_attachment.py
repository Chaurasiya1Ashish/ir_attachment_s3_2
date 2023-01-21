import pathlib
import base64
import logging
import mimetypes
import requests

from odoo import api, models

_logger = logging.getLogger(__name__)

def is_s3_bucket(bucket):
    meta = getattr(bucket, "meta", None)
    return meta and getattr(meta, "service_name", None) == "s3"


class IrAttachment(models.Model):

    _inherit = "ir.attachment"

    @api.depends("store_fname", "db_datas")
    def _compute_raw(self):
        url_records = self.filtered(lambda r: r.type == "url" and r.url)
        for attach in url_records:
            r = requests.get(attach.url, timeout=5)
            attach.raw = r.content

        super(IrAttachment, self - url_records)._compute_raw()


    def write(self, vals):
        if self.res_model and self.res_model in ['mail.compose.message', 'sale.order'] and self.type == 'url' and vals.get('datas'):
            vals = self._check_contents(vals)
            datas = vals.pop('datas', None)
            _logger.info("evt=IR_ATTACH method=write vals={}".format(vals))
            bucket = self.get_s3_bucket()
            related_values = self._get_datas_related_values_with_bucket(bucket, base64.b64decode(datas or b''), vals.get('mimetype'))
            vals['url'] = related_values['url']
            _logger.info("evt=IR_ATTACH method=write mimetype={}".format(vals.get('mimetype')))

        return super(IrAttachment, self).write(vals)

    @api.model_create_multi
    def create(self, vals_list):
        for values in vals_list:
            if values.get('type') != 'url' and values.get('res_model') in ['mail.compose.message', 'sale.order']:
                # convert Binary to Url
                values = self._check_contents(values)
                raw, datas = values.pop('raw', None), values.pop('datas', None)
                if raw or datas:
                    if isinstance(raw, str):
                        raw = raw.encode()

                bucket = self.get_s3_bucket()
                related_values = self._get_datas_related_values_with_bucket(bucket, raw or base64.b64decode(datas or b''), values.get('mimetype'))
                values['type'] = 'url'
                values['datas'] = False
                values['url'] = related_values['url']
                _logger.info("evt=IR_ATTACH method=create vals={}".format(values))

        return super(IrAttachment, self).create(vals_list)

    def _get_datas_related_values_with_bucket( self, bucket, bin_data, mimetype, checksum=None):
        bin_data = bin_data if bin_data else b""
        if not checksum:
            checksum = self._compute_checksum(bin_data)

        _logger.info("evt=IR_ATTACH method=_get_datas_related_values_with_bucket mimetype={}".format(mimetype))
        url = self._file_write_with_bucket(bucket, bin_data, mimetype, checksum)
        return {"url": url}


    def get_s3_bucket(self):
        bucket = self.env["res.config.settings"].get_s3_bucket()
        return bucket

    def _file_write_with_bucket(self, bucket, bin_data, mimetype, checksum):
        # make sure, that given bucket is s3 bucket
        _logger.info("evt=IR_ATTACH mimetype={}".format(mimetype))

        if not is_s3_bucket(bucket):
            return super(IrAttachment, self)._file_write_with_bucket(
                bucket, bin_data, mimetype, checksum
            )
        filename = checksum + mimetypes.guess_extension(mimetype, True) or ""

        bucket.put_object(
            Key=filename,
            Body=bin_data,
            ACL="public-read",
            ContentType=mimetype,
            ContentDisposition='attachment; filename="%s"' % filename,
        )

        _logger.info("evt=IR_ATTACH upload_path={}".format(filename))
        obj_url = self.env["res.config.settings"].get_s3_obj_url(bucket, filename)
        return obj_url
