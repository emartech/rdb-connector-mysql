package com.emarsys.rdb.connector.mysql

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.security.KeyStore
import java.security.cert.{Certificate, CertificateFactory}

import scala.util.Try

object CertificateUtil {

  def createKeystoreTempUrlFromCertificateString(certificate: String): Option[String] = {
    Try {
      val cert     = createCertificateFromString(certificate)
      val keyStore = createKeystoreWithCertificate(cert)
      val filePath = createKeystoreTempFile(keyStore)

      s"file:$filePath"
    }.toOption
  }

  private def createCertificateFromString(certificate: String): Certificate = {
    CertificateFactory
      .getInstance("X.509")
      .generateCertificate(
        new ByteArrayInputStream(certificate.getBytes)
      )
  }

  private def createKeystoreWithCertificate(certificate: Certificate, password: Array[Char] = Array.empty) = {
    val keyStore = KeyStore.getInstance("jks")
    keyStore.load(null, password)
    keyStore.setCertificateEntry("MySQLCACert", certificate)
    keyStore
  }

  private def createKeystoreTempFile(keyStore: KeyStore, password: Array[Char] = Array.empty) = {
    val temp = File.createTempFile("keystore", ".keystore")
    temp.deleteOnExit()

    val fos = new FileOutputStream(temp)
    keyStore.store(fos, password)
    fos.close()

    temp.getAbsolutePath
  }

}
