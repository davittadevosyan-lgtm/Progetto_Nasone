
﻿using Microsoft.Data.SqlClient;
using MQTTnet;
using MQTTnet.Client;
using System.Text;
using System.Text.Json;


string Broker = "broker.hivemq.com";
int Porta = 1883;
string Topic = "IAL/2IOT";

var factory = new MqttFactory();
var mqttClient = factory.CreateMqttClient();

const string connString = "Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=C:\\Users\\tadevosyand\\Downloads\\MQTTReciver-20260316T121012Z-3-001\\MQTTReciver\\DB_Nason.mdf;Integrated Security=True;Connect Timeout=30;Encrypt=True";

var options = new MqttClientOptionsBuilder()
    .WithTcpServer(Broker, Porta)
    .WithCleanSession()
    .Build();



mqttClient.ApplicationMessageReceivedAsync += async e =>
{
    string json = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
    Console.WriteLine($"/n Dati ricevuti: {json} ");



    try
    {
        var dati = JsonSerializer.Deserialize<NasoneDati>(json);
        if (dati == null) return;

        Console.WriteLine($"Temperatura: {dati.temperatura}°C");
        Console.WriteLine($"Umidità: {dati.umidita}%");
        Console.WriteLine($"Luce: {dati.luce} lux");
        Console.WriteLine($"CO2: {dati.co2} ppm");
        Console.WriteLine($"Qualità dell'aria: {dati.qualitaAria}");
        Console.WriteLine($"Suono: {dati.suono} dB");
        Console.WriteLine($"GPS: {dati.latitudine}, {dati.longitudine}");

        using var conn = new SqlConnection(connString);
        conn.Open();
        Console.WriteLine("Connesso a DB! ");

        string sql = @"INSERT INTO Letture (Timestamp, idDispositivo, Temperatura, Umidita, CO2, QualitaAria, Luce, Suono, Latitudine, Longitudine)
                     VALUES
                     (@timestamp, @idDev, @temp, @umidita, @co2, @aria, @luce, @suono, @lat, @lon)";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@timestamp", DateTime.Now);
        cmd.Parameters.AddWithValue("@idDev", dati.idDispositivo);
        cmd.Parameters.AddWithValue("@temp", dati.temperatura);
        cmd.Parameters.AddWithValue("@umidita", dati.umidita);
        cmd.Parameters.AddWithValue("@co2", dati.co2);
        cmd.Parameters.AddWithValue("@aria", dati.qualitaAria);
        cmd.Parameters.AddWithValue("@luce", dati.luce);
        cmd.Parameters.AddWithValue("@suono", dati.suono);
        cmd.Parameters.AddWithValue("@lat", dati.latitudine);
        cmd.Parameters.AddWithValue("@lon", dati.longitudine);

        cmd.ExecuteNonQuery();
        Console.WriteLine(sql);
        Console.WriteLine($"Salvato nel DB! Alle {DateTime.Now:HH:mm:ss}");
        Console.WriteLine("------------------------");
        Console.WriteLine("Premi Enter per DISCONNETTERTI..");

    }
    catch (Exception ex)
    {
        Console.WriteLine($" Errore: {ex.Message}");
    }

    await Task.CompletedTask;
};

await mqttClient.ConnectAsync(options);
Console.WriteLine("Connesso a Broker");

var emptyMessage = new MqttApplicationMessageBuilder()
    .WithTopic(Topic)
    .WithPayload("")
    .WithRetainFlag(true)
    .Build();

await mqttClient.PublishAsync(emptyMessage);


await mqttClient.SubscribeAsync(Topic);
Console.WriteLine($"In ascolto su: {Topic}\n");


Console.ReadLine();
await mqttClient.DisconnectAsync();
Console.WriteLine("Disconnesso.");


class NasoneDati
{
    public int idDispositivo;
    public float temperatura { get; set; }
    public float umidita { get; set; }
    public float luce { get; set; }
    public float co2 { get; set; }
    public string qualitaAria { get; set; }

    public float suono { get; set; }
    public float latitudine { get; set; }
    public float longitudine { get; set; }
}