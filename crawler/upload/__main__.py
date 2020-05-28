from tornado.options import define, options, parse_command_line

from . import uploaders

define("uploader", default="default", help="The uploader to use for reindexing.")
define("src-index", help="The index that the crawler writes to, typically the spider name.")
define("src-host", default="localhost:9200", help="Source index's ES addresss.")
define("dest-index", help="The index that will eventually be used for searching.")
define("dest-host", default="localhost:9200", help="Destination index's ES address.")

parse_command_line()

uploader = uploaders[options.uploader](
    src_index=options.src_index,
    src_host=options.src_host,
    dest_index=options.dest_index,
    dest_host=options.dest_host
)
uploader.upload()
