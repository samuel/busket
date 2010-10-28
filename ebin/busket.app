{application, busket,
 [
  {description, "Service monitoring"},
  {vsn, "1.1"},
  {modules, [
             busket_app,
             busket_sup,
             busket,
             busket_store,
             busket_store_debug,
             busket_store_mongo,
             busket_interface_udp
             % busket_web,
             % busket_web_sup,
             % busket_web_resource
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  sasl,
                  crypto,
                  emongo
                  % mochiweb,
                  % webmachine
                 ]},
  {mod, { busket_app, []}},
  {env, []}
 ]}.
