package com.shanta.taxifarepredictor

import android.os.Bundle
import android.telecom.Call
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import com.google.gson.Gson
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException


class MainActivity : AppCompatActivity() {


    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContentView(R.layout.activity_main)

        val pu_lat = findViewById<EditText>(R.id.pu_lat)
        val pu_lang = findViewById<EditText>(R.id.pu_lang)
        val do_lat = findViewById<EditText>(R.id.do_lat)
        val do_lang = findViewById<EditText>(R.id.do_lang)
        val button = findViewById<Button>(R.id.button)
        val pb = findViewById<ProgressBar>(R.id.progressBar)
        val output = findViewById<TextView>(R.id.output)

        button.setOnClickListener {

            pb.visibility = View.VISIBLE

            val jsonObject = mapOf(
                "PU_Longitude" to  pu_lang.text.toString(),
                "PU_Latitude" to pu_lat.text.toString(),
                "DO_Longitude" to do_lang.text.toString(),
                "DO_Latitude" to do_lat.text.toString()
            )
            val jsonString = Gson().toJson(jsonObject)

            // Create a JSON request body
            val requestBody = jsonString.toRequestBody("application/json".toMediaTypeOrNull())

            output.text = "Fare is 109.36$"

            // Create an OkHttpClient instance
            val client = OkHttpClient()
            val request = Request.Builder()
                .url("http://localhost/api/")
                .post(requestBody)
                .build()

            client.newCall(request).enqueue(object : Callback {


                override fun onFailure(call: Call, e: IOException) {

                    call.cancel()
                    runOnUiThread {
                        pb.visibility = View.INVISIBLE
                        output.text = "Bad request "
                    }

                }

                override fun onResponse(call: Call, response: Response) {

                    val myResponse = response.body!!.string()
                    runOnUiThread {
                        pb.visibility = View.INVISIBLE
                        output.text = "The fare is " + myResponse
                    }
                }

            })
        }

    }

}